const zmq = require('zeromq');
const EventEmitter = require('events');

class ZeroMQServer extends EventEmitter {
    listen(port, createReply) {
        this._createReply = createReply;
        return new Promise((resolve, reject) => {
            this._responder = zmq.socket('rep');
            this._responder.bind(`tcp://*:${port}`, (err) => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
            this._responder.on('message', (message) => {
                this.send(message);
            });
        });
    }

    send(message) {
        this._responder.send(this._createReply(message));
    }

    close() {
        this._responder.close();
    }
}

module.exports = { Server: ZeroMQServer };
