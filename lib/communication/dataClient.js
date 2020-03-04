const { Encoding } = require('@hkube/encoding');
const consts = require('../consts/messages').server;
const { Request } = require('./adapters/zeroMQ/request');


class DataRequest extends Encoding {
    constructor({ address, taskId, dataPath, encoding = 'json', timedOut = 600 }) {
        super({ type: encoding });
        const requestAdapter = new Request({ address, content: this.encode({ taskId, dataPath }), binary: encoding, timedOut });
        this.requestAdapter = requestAdapter;
        this.reply = new Promise((resolve) => {
            this.requestAdapter.on('message', (reply) => {
                resolve(this.decode(reply));
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
