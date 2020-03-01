
const { Request } = require('./adapters/zeroMQ/request');
const { Serializing } = require('../helpers/binaryEncoding');
const consts = require('../consts/messages').server;


class DataRequest extends Serializing {
    constructor({ address, taskId, dataPath, binary, timedOut = 600 }) {
        super({ binary });
        const requestAdapter = new Request({ address, content: this._stringify({ taskId, dataPath }), binary, timedOut });
        this.requestAdapter = requestAdapter;
        this.reply = new Promise((resolve) => {
            this.requestAdapter.on('message', (reply) => {
                resolve(this._parse(reply));
            });
            setTimeout(() => {
                if (!this.requestAdapter.isConnected()) resolve({ message: consts.notAvailable, reason: `server ${address.host}:${address.port} unreachable` });
            }, timedOut);
        });
    }

    async invoke() {
        await this.requestAdapter.invoke();
        return this.reply;
    }

    close() {
        this.requestAdapter.close();
    }
}

module.exports = { DataRequest };
