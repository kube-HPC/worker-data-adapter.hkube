
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
        await this._adapter.listen(this._port);
        console.log(`discovery serving on ${this._host}:${this._port} with ${this._encodingType} encoding`);
        this._init();
    }

    _init() {
        this._serving = 0;
        this._adapter.on('message', async (request) => {
            this._serving += 1;
            let result;
            try {
                const parsedReq = await this._encoding.decode(request);
                const { taskId, dataPath } = parsedReq;
                if (this._taskId !== taskId) {
                    result = this._createError(consts.notAvailable, `Current taskId is ${this._taskId}`);
                }
                else {
                    let data = this._data;
                    if (dataPath) {
                        data = objectPath.get(data, dataPath, 'DEFAULT');
                        if (data === 'DEFAULT') {
                            result = this._createError(consts.noSuchDataPath, `${dataPath} does not exist in data`);
                        }
                        else {
                            result = { data };
                        }
                    }
                    else {
                        result = { data };
                    }
                }
            }
            catch (e) {
                result = this._createError(consts.unknown, e.message);
            }
            finally {
                this._serving -= 1;
            }
            this._send(result);
        });
    }

    _createError(code, message) {
        return { error: { code, message } };
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
        return this._serving > 0;
    }

    _send(objToSend) {
        this._adapter.send(this._encoding.encode(objToSend));
    }

    close() {
        this._adapter.close();
    }
}
module.exports = DataServer;
