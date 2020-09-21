const zmq = require('zeromq');
const EventEmitter = require('events');
const { connectivity } = require('../../../consts/messages');

class ZeroMQServer extends EventEmitter {
    listen(port, createReply) {
        return new Promise((resolve, reject) => {
            this._socket = zmq.socket('rep');
            this._socket.on('message', (m) => this._onMessage(m, createReply));
            this._socket.bind(`tcp://*:${port}`, (err) => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    async _onMessage(message, createReply) {
        this._lastServing = Date.now();
        if (message.toString() === connectivity.ping) {
            await this._send(connectivity.pong);
        }
        else {
            await this.send(message, createReply);
        }
        this._lastServing = Date.now();
    }

    isServing() {
        if ((this._lastServing) && (Date.now() - this._lastServing < 10000)) {
            return true;
        }
        return false;
    }

    async send(message, createReply) {
        const reply = createReply(message);
        await this._send(reply);
    }

    _send(message) {
        return new Promise((resolve, reject) => {
            this._socket.send(message, null, (err, data) => {
                if (err) {
                    return reject(err);
                }
                return resolve(data);
            });
        });
    }

    close() {
        this._socket.close();
    }
}

module.exports = { Server: ZeroMQServer };
