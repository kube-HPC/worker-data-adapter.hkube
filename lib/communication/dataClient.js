const { Encoding } = require('@hkube/encoding');
const consts = require('../consts/messages').server;
const { Request } = require('./adapters/zeroMQ/request');


class DataRequest {
    constructor({ address, taskId, dataPath, encoding, timedOut = 600 }) {
        this._encoding = new Encoding({ type: encoding });
        const requestAdapter = new Request({ address, content: this._encoding.encode({ taskId, dataPath }), timedOut });
        this._requestAdapter = requestAdapter;
        this._reply = new Promise((resolve) => {
            this._requestAdapter.on('message', (reply) => {
                resolve(this._encoding.decode(reply, { customEncode: true }));
            });
            setTimeout(() => {
                if (!this._requestAdapter.isConnected()) resolve({ error: { code: consts.notAvailable, message: `server ${address.host}:${address.port} unreachable` } });
            }, timedOut);
        });
    }

    async invoke() {
        await this._requestAdapter.invoke();
        return this._reply;
    }

    close() {
        this._requestAdapter.close();
    }
}

module.exports = { DataRequest };
