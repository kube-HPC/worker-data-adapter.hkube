
const deep = require('deep-get-set');
const consts = require('../consts/messages').server;
const { Serializing } = require('../helpers/binaryEncoding');
const { Server } = require('./adapters/zeroMQ/server');


class DataServer extends Serializing {
    constructor({ port, binary }) {
        super({ binary });
        this.server = new Server({ port });
        this.init();
        this.endSendingState();
    }

    setSendingState(taskId, data) {
        this.taskId = taskId;
        this.data = data;
    }

    endSendingState() {
        this.taskId = null;
        this.data = null;
    }

    init() {
        this.serving = 0;

        this.server.on('message', async (request) => {
            this.serving += 1;
            try {
                const parsedReq = await this._parse(request);
                const { taskId, dataPath } = parsedReq;
                if (this.taskId !== taskId) {
                    this._send({ message: consts.notAvailable, reason: `Current taskId is ${this.taskId}` });
                    return;
                }
                let toSend = this.data;
                if (dataPath) {
                    toSend = deep(toSend, dataPath);
                }
                if (toSend === undefined) {
                    this._send({ error: consts.noSuchDataPath, reason: `${dataPath} does not exist in data` });
                    return;
                }
                this._send({ message: consts.success, data: toSend });
            }
            finally {
                this.serving -= 1;
            }
        });
    }

    isServing() {
        return this.serving > 0;
    }

    _send(objToSend) {
        this.server.send(this._stringify(objToSend));
    }

    close() {
        this.server.close();
    }
}
module.exports = DataServer;
