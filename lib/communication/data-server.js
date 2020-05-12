const EventEmitter = require('events');
const objectPath = require('object-path');
const { Encoding } = require('@hkube/encoding');
const consts = require('../consts/messages').server;
const { Server } = require('./adapters/zeroMQ/server');
const Cache = require('./data-server-cache');


class DataServer extends EventEmitter {
    constructor({ host, port, encoding, maxCacheSize }) {
        super();
        this._cache = new Cache({ maxCacheSize });
        this._adapter = new Server({ port });
        this._host = host;
        this._port = port;
        this._encodingType = encoding;
        this._encoding = new Encoding({ type: encoding });
    }

    async listen() {
        await this._adapter.listen(this._port, (m) => this._createReply(m));
        console.log(`discovery serving on ${this._host}:${this._port} with ${this._encodingType} encoding`);
    }

    _createReply(message) {
        let result;
        try {
            const decodedMessage = this._encoding.decode(message);
            result = this.createData(decodedMessage);
        }
        catch (e) {
            result = this._createError(consts.unknown, e.message);
        }
        return this._encoding.encode(result, { customEncode: true });
    }

    createData(message) {
        const { tasks, taskId, dataPath } = message;
        if (taskId) {
            return this.getDataByTaskId(taskId, dataPath);
        }
        let errors = false;
        const items = [];
        tasks.forEach(t => {
            const result = this.getDataByTaskId(t, dataPath);
            if (result && result.hkube_error) {
                errors = true;
            }
            items.push(result);
        });
        return { items, errors };
    }

    getDataByTaskId(taskId, dataPath) {
        let result;
        if (!this._cache.has(taskId)) {
            result = this._createError(consts.notAvailable, 'taskId notAvailable');
        }
        else {
            const data = this._cache.get(taskId);
            result = this._createDataByPath(data, dataPath);
        }
        return result;
    }

    _createDataByPath(data, dataPath) {
        let result = data;
        if (dataPath) {
            result = objectPath.get(data, dataPath, 'DEFAULT');
            if (result === 'DEFAULT') {
                result = this._createError(consts.noSuchDataPath, `${dataPath} does not exist in data`);
            }
        }
        return result;
    }

    _createError(code, message) {
        return { hkube_error: { code, message } };
    }

    setSendingState(taskId, data) {
        this._cache.update(taskId, data);
    }

    isServing() {
        return this._adapter.isServing();
    }

    async waitTillServingIsDone() {
        const sleep = (ms) => {
            return new Promise(resolve => setTimeout(resolve, ms));
        };
        while (this.isServing()) {
            // eslint-disable-next-line no-await-in-loop
            await sleep(100);
        }
    }

    _send(objToSend) {
        this._adapter.send(this._encoding.encode(objToSend));
    }

    close() {
        this._adapter.close();
    }
}
module.exports = DataServer;
