const zmq = require('zeromq');
const EventEmitter = require('events');

class ZeroMQServer extends EventEmitter {
    listen(port, createReply) {
        this._createReply = createReply;
        this.numberOfConn = 0;
        return new Promise((resolve, reject) => {
            this._responder = zmq.socket('rep');
            this._responder.monitor();

            this._responder.on('message', async (message) => {
                this.numberOfConn += 1;
                await this.send(message);
            });
            this._responder.on('disconnect', async () => {
                this.numberOfConn -= 1;
            });
            this._responder.bind(`tcp://*:${port}`, (err) => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    isServing() {
        return this.numberOfConn > 0;
    }

    send(message) {
        this._responder.send(this._createReply(message));
    }

    close() {
        this._responder.close();
    }
}

module.exports = { Server: ZeroMQServer };
