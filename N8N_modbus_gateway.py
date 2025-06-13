# -*- coding: utf-8 -*-
import json
import os
import sys
import time
from datetime import datetime
from pymodbus.client import ModbusTcpClient
from pymodbus.constants import Endian

# 將字串對應到 pymodbus 的 Endian 常數
BYTE_ORDER_MAP = {
    "big": Endian.BIG,
    "little": Endian.LITTLE
}

# === 主程式 ===

def load_config():
    """從環境變數、檔案路徑或預設的 points.json 載入配置"""
    if os.environ.get("MODBUS_CONFIG_JSON"):
        return json.loads(os.environ["MODBUS_CONFIG_JSON"])

    if len(sys.argv) > 1:
        with open(sys.argv[1], "r") as f:
            return json.load(f)

    with open("points.json", "r") as f:
        return json.load(f)

def validate_config(config):
    """簡單驗證所需配置結構"""
    if not isinstance(config.get("commands"), list):
        raise ValueError("無效配置: 'commands' 必須是列表。")
    
    for cmd in config["commands"]:
        if "function_code" not in cmd or "address" not in cmd or "quantity" not in cmd:
            raise ValueError("無效指令: 缺少必要欄位 (function_code, address, quantity)")
        if "fields" not in cmd or not isinstance(cmd["fields"], list):
            raise ValueError("無效指令: 'fields' 必須是列表")

def main():
    config = load_config()
    validate_config(config)

    # 載入全域設定
    MODBUS_HOST = config.get("ip", "127.0.0.1")
    MODBUS_PORT = config.get("port", 502)
    UNIT_ID = config.get("unit_id", 1)
    POLL_INTERVAL = config.get("poll_interval", 10)
    HEARTBEAT_INTERVAL = config.get("heartbeat_interval", 60)
    DEFAULT_BYTE_ORDER = BYTE_ORDER_MAP.get(config.get("byte_order", "big"), Endian.BIG)
    DEFAULT_WORD_ORDER = BYTE_ORDER_MAP.get(config.get("word_order", "big"), Endian.BIG)
    commands = config.get("commands", [])

    # 連線重試機制（無限次嘗試）
    RETRY_DELAY = 3  # 秒
    
    client = None
    attempt = 0
    
    while True:
        attempt += 1
        print(f"嘗試連線到 Modbus 伺服器 (第 {attempt} 次): {MODBUS_HOST}:{MODBUS_PORT}", flush=True)
        
        try:
            # 創建新的客戶端實例
            client = ModbusTcpClient(MODBUS_HOST, port=MODBUS_PORT)
            
            if client.connect():
                print(f"成功連線到 Modbus 伺服器: {MODBUS_HOST}:{MODBUS_PORT}", flush=True)
                # 輸出連線成功訊息到 N8N 訊息流
                success_msg = {
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "source": "modbus_gateway",
                    "type": "connection_success",
                    "message": f"成功連線到 Modbus 伺服器: {MODBUS_HOST}:{MODBUS_PORT}",
                    "attempt": attempt
                }
                print(json.dumps(success_msg, ensure_ascii=False), flush=True)
                break
            else:
                # 輸出連線失敗訊息到 N8N 訊息流
                failure_msg = {
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "source": "modbus_gateway",
                    "type": "connection_failed",
                    "message": f"連線失敗: 無法連接到 {MODBUS_HOST}:{MODBUS_PORT}",
                    "attempt": attempt,
                    "retry_in_seconds": RETRY_DELAY
                }
                print(json.dumps(failure_msg,ensure_ascii=False), flush=True )
                
        except Exception as e:
            # 輸出連線異常訊息到 N8N 訊息流
            error_msg = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "source": "modbus_gateway",
                "type": "connection_error",
                "message": f"連線異常: {str(e)}",
                "error_type": type(e).__name__,
                "attempt": attempt,
                "retry_in_seconds": RETRY_DELAY
            }
            print(json.dumps(error_msg, ensure_ascii=False) ,flush=True)
        
        # 清除客戶端資源
        if client:
            try:
                client.close()
            except:
                pass
            client = None
        
        # 等待後重試
        print(f"等待 {RETRY_DELAY} 秒後重試...", flush=True)
        time.sleep(RETRY_DELAY)

    # 用於儲存上一次成功讀取的值
    last_values = {}
    # 記錄上次心跳時間
    last_heartbeat = time.time()
    # 首次運行標誌，用於第一次讀取時推送所有數據
    first_run = True
    # 從環境變數檢查是否只運行一次
    run_once = os.environ.get("RUN_ONCE") == "1"

    while True:
        now = time.time()
        # 儲存本次輪詢中讀取到的所有值
        current_values = {}
        # 儲存本次輪詢中發生的錯誤
        errors = {}
        
        # 儲存本次輪詢中發生變化的值，僅用於推送
        changed_values = {} 

        for command in commands:
            try:
                # 一次讀取整個命令範圍的數據
                raw_data = read_command(client, command, UNIT_ID, DEFAULT_BYTE_ORDER, DEFAULT_WORD_ORDER)
                
                # 將讀取的數據分派到各個欄位
                for field in command["fields"]:
                    field_name = field["name"]
                    try:
                        value = extract_field_value(raw_data, field, command)
                        current_values[field_name] = value
                        
                        # 檢查值是否變動或是否為首次運行 (首次運行時視為所有值都變動)
                        if first_run or last_values.get(field_name) != value:
                            changed_values[field_name] = value
                            
                    except Exception as e:
                        current_values[field_name] = None
                        errors[field_name] = {
                            "type": type(e).__name__,
                            "message": str(e)
                        }
                        
            except Exception as e:
                # 整個命令失敗，所有相關欄位都設為 None 並記錄錯誤
                for field in command["fields"]:
                    field_name = field["name"]
                    current_values[field_name] = None
                    errors[field_name] = {
                        "type": type(e).__name__,
                        "message": f"指令失敗: {str(e)}"
                    }

        # **數據推送邏輯**
        # 只有當 changed_values 不為空時才推送 (即有數據變動)
        if changed_values: 
            output = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "source": "modbus_gateway",
                "type": "data",
                "values": changed_values, # 推送變動的值
            }
            if errors:
                output["errors"] = errors
            print(json.dumps(output,ensure_ascii=False), flush=True)

        # 更新 last_values，只更新成功取得的值，以便下次比較
        for key, value in current_values.items():
            if value is not None:
                last_values[key] = value

        # 首次運行標誌設定為 False，表示之後的循環將進行數據比較
        first_run = False

        # **心跳推送邏輯**
        # 當前時間與上次心跳時間之差超過心跳間隔時觸發
        if now - last_heartbeat >= HEARTBEAT_INTERVAL:
            hb = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "source": "modbus_gateway",
                "type": "heartbeat",
                "values": current_values # 心跳時推送所有當前值
            }
            if errors:
                hb["errors"] = errors
            print(json.dumps(hb, ensure_ascii=False), flush=True)
            last_heartbeat = now

        # 如果設定為只運行一次，則退出循環
        if run_once:
            break

        # 等待下一次輪詢
        time.sleep(POLL_INTERVAL)

# === 輔助函數 ===

def read_command(client, command, unit_id, default_byte_order, default_word_order):
    """執行單一 Modbus 命令，返回原始數據"""
    fc = command["function_code"]
    addr = command["address"]
    qty = command["quantity"]

    if fc == 1:  # 讀取線圈 (Read Coils)
        rr = client.read_coils(address=addr, count=qty, slave=unit_id)
        if rr.isError():
            raise Exception(f"Modbus 錯誤 (FC01): {str(rr)}")
        return rr.bits
    
    elif fc == 2:  # 讀取離散輸入 (Read Discrete Inputs)
        rr = client.read_discrete_inputs(address=addr, count=qty, slave=unit_id)
        if rr.isError():
            raise Exception(f"Modbus 錯誤 (FC02): {str(rr)}")
        return rr.bits

    elif fc == 3:  # 讀取保持暫存器 (Read Holding Registers)
        rr = client.read_holding_registers(address=addr, count=qty, slave=unit_id)
        if rr.isError():
            raise Exception(f"Modbus 錯誤 (FC03): {str(rr)}")
        return rr.registers

    elif fc == 4:  # 讀取輸入暫存器 (Read Input Registers)
        rr = client.read_input_registers(address=addr, count=qty, slave=unit_id)
        if rr.isError():
            raise Exception(f"Modbus 錯誤 (FC04): {str(rr)}")
        return rr.registers

    else:
        raise Exception(f"不支援的功能碼: {fc}")

def extract_field_value(raw_data, field, command):
    """從原始數據中提取特定欄位的值"""
    fc = command["function_code"]
    offset = field["offset"]
    datatype = field["datatype"]
    
    # 獲取欄位特定的字節序和字序，如果未指定則使用預設值
    byte_order = BYTE_ORDER_MAP.get(field.get("byte_order", "big"), Endian.BIG)
    word_order = BYTE_ORDER_MAP.get(field.get("word_order", "big"), Endian.BIG)
    
    if fc in [1, 2]:  # 線圈或離散輸入
        if datatype == "bool":
            return bool(raw_data[offset])
        elif datatype == "uint16":
            return int(raw_data[offset])
        else:
            raise Exception(f"FC{fc:02d} 不支援的數據類型 '{datatype}'")
    
    elif fc in [3, 4]:  # 保持暫存器或輸入暫存器
        if datatype == "uint16":
            return raw_data[offset]
        elif datatype == "int16":
            value = raw_data[offset]
            return value if value < 32768 else value - 65536
        elif datatype == "uint32":
            if offset + 1 >= len(raw_data):
                raise Exception("uint32 數據不足")
            if word_order == Endian.BIG:
                return (raw_data[offset] << 16) | raw_data[offset + 1]
            else:
                return (raw_data[offset + 1] << 16) | raw_data[offset]
        elif datatype == "int32":
            if offset + 1 >= len(raw_data):
                raise Exception("int32 數據不足")
            if word_order == Endian.BIG:
                value = (raw_data[offset] << 16) | raw_data[offset + 1]
            else:
                value = (raw_data[offset + 1] << 16) | raw_data[offset]
            return value if value < 2147483648 else value - 4294967296
        elif datatype == "float32":
            if offset + 1 >= len(raw_data):
                raise Exception("float32 數據不足")
            # 使用 pymodbus 的轉換功能
            registers = raw_data[offset:offset + 2]
            # 創建臨時客戶端用於轉換 (不會實際連線，只用於轉換)
            temp_client = ModbusTcpClient('localhost')
            return temp_client.convert_from_registers(
                registers,
                temp_client.DATATYPE.FLOAT32,
                word_order=word_order
            )
        elif datatype == "string":
            # 字串處理
            length = field.get("length", 1)  # 預設1個暫存器 = 2個字符
            if offset + length > len(raw_data):
                raise Exception("字串數據不足")
            
            string_bytes = []
            for i in range(length):
                reg = raw_data[offset + i]
                if byte_order == Endian.BIG:
                    string_bytes.extend([reg >> 8, reg & 0xFF])
                else:
                    string_bytes.extend([reg & 0xFF, reg >> 8])
            
            # 移除空字符並轉換為字串
            string_bytes = [b for b in string_bytes if b != 0]
            return bytes(string_bytes).decode('ascii', errors='ignore')
        else:
            raise Exception(f"不支援的數據類型: {datatype}")
    
    else:
        raise Exception(f"不支援的功能碼: {fc}")


if __name__ == "__main__":
    main()
