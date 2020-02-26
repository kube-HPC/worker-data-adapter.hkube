const zmq = require('zeromq');
const deep = require('deep-get-set');
const consts = require('../consts/messages').server;
const { Serializing } = require('../helpers/binaryEncoding');


class DataServer extends Serializing {
    constructor({ port, binary }) {
        super({ binary });
        this.init(port);
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

    init(port) {
        this.responder = zmq.socket('rep');
        this.serving = 0;
        this.responder.bind(`tcp://*:${port}`, (err) => {
            if (err) {
                // console.log(err);
            }
            else {
                // console.log(`Listening on ${port}...`);
            }
        });
        this.responder.on('message', async (request) => {
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
        process.on('SIGINT', () => {
            try {
                this.responder.close();
            }
            // eslint-disable-next-line no-empty
            catch (e) {

            }
        });
    }

    isServing() {
        return this.serving > 0;
    }

    _send(objToSend) {
        this.responder.send(this._stringify(objToSend));
    }

    close() {
        this.responder.close();
    }
}
module.exports = DataServer;
