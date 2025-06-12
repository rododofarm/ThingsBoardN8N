# ThingsBoard Modbus Gateway Node

This repository contains a custom n8n node that executes the provided
`N8N_modbus_gateway.py` script. The node is written in TypeScript and needs to
be compiled before it can be used in n8n.

## Setup

1. Install dependencies:

   ```bash
   npm install
   ```

2. Compile the TypeScript source:

   ```bash
   npm run build
   ```

   The compiled files will be generated in the `dist` directory.

3. Copy or symlink the `dist` folder to your n8n custom extensions directory
   or point the `N8N_CUSTOM_EXTENSIONS` environment variable to this repository.
   After that, start n8n normally:

   ```bash
   n8n
   ```

The `ModbusGateway` node should then be available inside your n8n instance.
