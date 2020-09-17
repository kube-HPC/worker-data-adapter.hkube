const { Encoding } = require('@hkube/encoding');
const { Request } = require('./adapters/zeroMQ/request');
const consts = require('../consts/messages').server;

class DataRequest {
    constructor({ address, tasks, encoding, timeout, networkTimeout }) {
        this._tasks = tasks;
        this._encoding = new Encoding({ type: encoding });
        const content = this._encoding.encode({ tasks });
        this._adapter = new Request({ address, content, timeout, networkTimeout });
    }

    async invoke() {
        try {
            console.log(this._adapter.address);
            const responseFrames = await this._adapter.invoke();
            const results = [];
            const length = responseFrames.length / 2;

            for (let i = 0; i < length; i += 1) {
                const header = responseFrames[i * 2];
                const content = responseFrames[(i * 2) + 1];
                const decoded = this._encoding.decodeHeaderPayload(header, content);
                results.push({ size: content.length, content: decoded });
            }
            return results;
        }
        catch (e) {
            console.error(e.message);
            const code = e.code || consts.unknown;
            const results = this._tasks.map(() => ({ size: 0, content: this._createError(code, e.message) }));
            return results;
        }
        finally {
            this._adapter.close();
        }
    }

    _createError(code, message) {
        return { hkube_error: { code, message } };
    }
}

module.exports = { DataRequest };
