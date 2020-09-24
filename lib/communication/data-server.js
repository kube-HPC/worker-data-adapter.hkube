const EventEmitter = require('events');
const { Encoding } = require('@hkube/encoding');
const consts = require('../consts/messages').server;
const { Server } = require('./adapters/zeroMQ/server');
const Cache = require('../cache/cache');

class DataServer extends EventEmitter {
    constructor({ host, port, encoding, maxCacheSize }) {
        super();
        this._cache = new Cache({ maxCacheSize });
        this._adapter = new Server({ port });
        this._host = host;
        this._port = port;
        this._encodingType = encoding;
        this._encoding = new Encoding({ type: encoding });
        const encodedError = this._createError(consts.notAvailable, 'taskId notAvailable');
        const { header, payload } = this._encoding.encodeHeaderPayload(encodedError);
        this._notAvailable = { header, value: payload };
    }

    async listen() {
        await this._adapter.listen(this._port, (m) => this._createReply(m));
        console.log(`discovery serving on ${this._host}:${this._port} with ${this._encodingType} encoding`);
    }

    _createReply(message) {
        let result;
        try {
            const decodedMessage = this._encoding.decode(message);
            result = this.getDataByTasks(decodedMessage.tasks);
        }
        catch (e) {
            const error = this._createError(consts.unknown, e.message);
            const { header, payload } = this._encoding.encodeHeaderPayload(error);
            return [header, payload];
        }
        const parts = [];
        result.forEach(r => {
            parts.push(r.header);
            parts.push(r.value);
        });
        return parts;
    }

    getDataByTasks(tasks) {
        const results = [];
        tasks.forEach(t => {
            let result;
            if (!this._cache.has(t)) {
                result = this._notAvailable;
            }
            else {
                result = this._cache.getWithHeader(t);
            }
            results.push(result);
        });
        return results;
    }

    _createError(code, message) {
        return { hkube_error: { code, message } };
    }

    setSendingState(taskId, data, size, header) {
        return this._cache.update(taskId, data, size, header);
    }

    isServing() {
        return this._adapter.isServing();
    }

    close() {
        this._adapter.close();
    }
}
module.exports = DataServer;
