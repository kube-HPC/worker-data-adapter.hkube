const { Encoding } = require('@hkube/encoding');
const consts = require('../consts/messages').server;
const { Request } = require('./adapters/zeroMQ/request');


class DataRequest {
    constructor({ address, tasks, taskId, dataPath, encoding, timedOut = 600 }) {
        this._encoding = new Encoding({ type: encoding });
        const requestAdapter = new Request({ address, content: this._encoding.encode({ tasks, taskId, dataPath }), timedOut });
        this._requestAdapter = requestAdapter;
        this._reply = new Promise((resolve) => {
            this._requestAdapter.on('message', (reply) => {
                resolve(this._encoding.decode(reply, { customEncode: true }));
            });
            this._requestAdapter.on('error', (errorMessage) => {
                resolve({ hkube_error: { code: consts.unknown, message: errorMessage } });
            });
            setTimeout(() => {
                if (!this._requestAdapter.isConnected()) {
                    this._requestAdapter.close();
                    resolve({
                        hkube_error: {
                            code: consts.notAvailable,
                            message: `server ${address.host}:${address.port} unreachable`
                        }
                    });
                }
            }, timedOut);
        });
    }

    async invoke() {
        await this._requestAdapter.invoke();
        const rep = await this._reply;
        return rep;
    }

    close() {
        this._requestAdapter.close();
    }
}

module.exports = { DataRequest };
