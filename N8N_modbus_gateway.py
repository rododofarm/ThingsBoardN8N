import json
import os
import sys
import time
from datetime import datetime
from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

# 對應字串到 pymodbus 的 Endian 常數
BYTE_ORDER_MAP = {
    "big": Endian.Big,
    "little": Endian.Little
}

# === 主程式 ===

def load_config():
    """Load configuration from env variable, file path or default points.json"""
    if os.environ.get("MODBUS_CONFIG_JSON"):
        return json.loads(os.environ["MODBUS_CONFIG_JSON"])

    if len(sys.argv) > 1:
        with open(sys.argv[1], "r") as f:
            return json.load(f)

    with open("points.json", "r") as f:
        return json.load(f)


def main():
    config = load_config()

    # 載入全域設定
    MODBUS_HOST = config.get("modbus_host", "127.0.0.1")
    MODBUS_PORT = config.get("modbus_port", 502)
    UNIT_ID = config.get("unit_id", 1)
    POLL_INTERVAL = config.get("poll_interval", 10)
    HEARTBEAT_INTERVAL = config.get("heartbeat_interval", 60)
    DEFAULT_BYTE_ORDER = BYTE_ORDER_MAP.get(config.get("byte_order", "big"), Endian.Big)
    DEFAULT_WORD_ORDER = BYTE_ORDER_MAP.get(config.get("word_order", "big"), Endian.Big)
    points = config.get("points", [])

    client = ModbusTcpClient(MODBUS_HOST, port=MODBUS_PORT)
    last_values = {}
    last_heartbeat = time.time()
    first_run = True
    run_once = os.environ.get("RUN_ONCE") == "1"

    while True:
        now = time.time()
        current_values = {}
        errors = {}
        changed = False

        for point in points:
            name = point["name"]
            try:
                value = read_point(client, point, UNIT_ID, DEFAULT_BYTE_ORDER, DEFAULT_WORD_ORDER)
                current_values[name] = value
                if first_run or last_values.get(name) != value:
                    changed = True
            except Exception as e:
                current_values[name] = None
                errors[name] = str(e)

        if changed or errors:
            output = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "source": "modbus_gateway",
                "type": "data",
                "values": current_values,
            }
            if errors:
                output["errors"] = errors
            print(json.dumps(output), flush=True)

            for key, value in current_values.items():
                if value is not None:
                    last_values[key] = value

            first_run = False

        if now - last_heartbeat >= HEARTBEAT_INTERVAL:
            hb = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "source": "modbus_gateway",
                "type": "heartbeat"
            }
            if errors:
                hb["errors"] = errors
            print(json.dumps(hb), flush=True)
            last_heartbeat = now

        if run_once:
            break

        time.sleep(POLL_INTERVAL)

# === 輔助函數 ===

def read_point(client, point, unit_id, default_byte_order, default_word_order):
    fc = point["function_code"]
    addr = point["address"]
    qty = point["quantity"]
    dtype = point["datatype"]

    byte_order = BYTE_ORDER_MAP.get(point.get("byte_order", "big"), default_byte_order)
    word_order = BYTE_ORDER_MAP.get(point.get("word_order", "big"), default_word_order)

    if fc == 2:
        rr = client.read_discrete_inputs(addr, qty, unit=unit_id)
        if rr.isError():
            raise Exception("Modbus error (FC02): " + str(rr))
        return bool(rr.bits[0]) if qty == 1 else rr.bits

    elif fc == 3:
        rr = client.read_holding_registers(addr, qty, unit=unit_id)
    elif fc == 4:
        rr = client.read_input_registers(addr, qty, unit=unit_id)
    else:
        raise Exception(f"Unsupported function code: {fc}")

    if rr.isError():
        raise Exception(f"Modbus error (FC{fc:02d}): {rr}")

    decoder = BinaryPayloadDecoder.fromRegisters(rr.registers, byteorder=byte_order, wordorder=word_order)

    if dtype == "float32":
        return decoder.decode_32bit_float()
    elif dtype == "int16":
        return decoder.decode_16bit_int()
    elif dtype == "uint16":
        return decoder.decode_16bit_uint()
    elif dtype == "int32":
        return decoder.decode_32bit_int()
    elif dtype == "uint32":
        return decoder.decode_32bit_uint()
    else:
        return rr.registers if qty > 1 else rr.registers[0]

if __name__ == "__main__":
    main()
