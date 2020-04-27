
const EventEmitter = require('events');
const objectPath = require('object-path');
const { Encoding } = require('@hkube/encoding');
const consts = require('../consts/messages').server;
const { Server } = require('./adapters/zeroMQ/server');


class DataServer extends EventEmitter {
    constructor({ host, port, encoding }) {
        super();
        this._adapter = new Server({ port });
        this._taskId = null;
        this._data = null;
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
        return result;
    }

    createData(message) {
        const { taskId, dataPath } = message;
        let result = null;
        if (this._taskId !== taskId) {
            result = this._createError(consts.notAvailable, `Current taskId is ${this._taskId}`);
        }
        else {
            result = this._data;
            if (dataPath) {
                const data = this._encoding.decode(this._data);
                result = objectPath.get(data, dataPath, 'DEFAULT');
                if (result === 'DEFAULT') {
                    result = this._createError(consts.noSuchDataPath, `${dataPath} does not exist in data`);
                }
                else {
                    result = this._encoding.encode(result);
                }
            }
        }
        return result;
    }

    _createError(code, message) {
        return this._encoding.encode({ error: { code, message } });
    }

    setSendingState(taskId, data) {
        this._taskId = taskId;
        this._data = data;
    }

    endSendingState() {
        this._taskId = null;
        this._data = null;
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
