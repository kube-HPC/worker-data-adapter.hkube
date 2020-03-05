const zmq = require('zeromq');
const EventEmitter = require('events');

class ZeroMQServer extends EventEmitter {
    constructor({ port }) {
        super();
        setImmediate(() => this.init(port));
    }

    init(port) {
        this.responder = zmq.socket('rep');
        this.responder.bind(`tcp://*:${port}`, (err) => {
            if (err) {
                this.emit('bind-error', err);
            }
            else {
                this.emit('bind', port);
            }
        });
        this.responder.on('message', async (request) => {
            this.emit('message', request);
        });
    }

    send(objToSend) {
        if (Buffer.isBuffer(objToSend)) {
            const buffLength = objToSend.length;
            const chunks = [];
            let i = 0;
            while (i < buffLength) {
                chunks.push(objToSend.slice(i, i += (1024 * 1024)));
            }
            this.responder.send(chunks);
            return;
        }
        this.responder.send(objToSend);
    }

    close() {
        this.responder.close();
    }
}

module.exports = { Server: ZeroMQServer };
