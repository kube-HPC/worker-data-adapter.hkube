const zmq = require('zeromq');
const EventEmitter = require('events');


class ZeroMQRequest extends EventEmitter {
    constructor({ address, timedOut = 600, content }) {
        super();
        this.done = false;
        this.content = content;
        this.port = address.port;
        this.host = address.host;
        this.requester = zmq.socket('req');
        this.connected = false;
        this.timedOut = false;
        this.requester.monitor(timedOut - 5, 0);
        this.requester.on('connect', () => {
            this.connected = true;
        });
        this.requester.connect(`tcp://${address.host}:${address.port}`);
        this.requester.on('message', (message) => {
            this.connected = true;
            this.done = true;
            this.close();
            this.emit('message', message);
        });
        this.requester.on('disconnect', () => {
            if (!this.done) {
                this.emit('error', 'early disconnect');
            }
        });
    }

    isConnected() {
        return this.connected;
    }

    async invoke() {
        await this.requester.send(this.content);
        return this.reply;
    }

    close() {
        this.requester.unmonitor();
        this.requester.close();
    }
}
module.exports = { Request: ZeroMQRequest };
