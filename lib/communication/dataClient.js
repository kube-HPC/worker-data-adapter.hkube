const zmq = require('zeromq');
const { Serializing } = require('../helpers/binaryEncoding');
const consts = require('../consts/messages').server;

class DataRequest extends Serializing {
    constructor({ port, host, taskId, dataPath, binary, timedOut = 600 }) {
        super({ binary });
        this.port = port;
        this.host = host;
        this.taskId = taskId;
        this.dataPath = dataPath;
        this.requester = zmq.socket('req');
        this.connected = false;
        this.timedOut = false;
        this.requester.monitor(timedOut - 5, 0);
        this.requester.on('connect', () => {
            this.connected = true;
        });
        this.requester.connect(`tcp://${host}:${port}`);
        this.reply = new Promise((resolve) => {
            this.requester.on('message', (reply) => {
                this.connected = true;
                this.requester.close();
                resolve(this._parse(reply));
            });
            setTimeout(() => {
                if (!this.connected) resolve({ message: consts.notAvailable, reason: `server ${host}:${port} unreachable` });
            }, timedOut);
        });
    }

    async invoke() {
        const { dataPath, taskId } = this;
        await this.requester.send(this._stringify({ taskId, dataPath }));
        return this.reply;
    }

    close() {
        this.requester.close();
    }
}

module.exports = DataRequest;
