import { IExecuteFunctions } from 'n8n-core';
import { INodeType, INodeTypeDescription, NodeOperationError } from 'n8n-workflow';
import { exec } from 'child_process';
import * as path from 'path';
import { promisify } from 'util';

export class ModbusGateway implements INodeType {
    description: INodeTypeDescription = {
        displayName: 'ModbusGateway',
        name: 'modbusGateway',
        group: ['transform'],
        version: 1,
        description: 'Execute Modbus gateway Python script',
        defaults: {
            name: 'ModbusGateway',
        },
        inputs: ['main'],
        outputs: ['main'],
        properties: [
            {
                displayName: 'Configuration JSON',
                name: 'pointsJson',
                type: 'string',
                default: '',
                description: 'Modbus points configuration in JSON format',
            },
        ],
    };

    async execute(this: IExecuteFunctions) {
        const pointsJson = this.getNodeParameter('pointsJson', 0) as string;
        // Resolve path to Python script relative to the compiled dist directory
        // so the gateway works when distributed.
        const scriptPath = path.resolve(
            __dirname,
            '../../../N8N_modbus_gateway.py',
        );
        const execAsync = promisify(exec);
        let stdout: string;
        try {
            const { stdout: out } = await execAsync(`python3 ${scriptPath}`, {
                env: {
                    ...process.env,
                    MODBUS_CONFIG_JSON: pointsJson,
                    RUN_ONCE: '1',
                },
            });
            stdout = out;
        } catch (error) {
            const stderr = (error as any).stderr || (error as Error).message;
            throw new NodeOperationError(this.getNode(), `Python process error: ${stderr}`);
        }

        const lines = stdout.trim().split(/\r?\n/);
        const last = lines[lines.length - 1];
        let data: any;
        try {
            data = JSON.parse(last);
        } catch (e) {
            data = { raw: stdout };
        }
        return [
            {
                json: data,
            },
        ];
    }
}
