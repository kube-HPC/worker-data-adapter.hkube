const zmq = require('zeromq');
const EventEmitter = require('events');
const { connectivity } = require('../../../consts/messages');

class ZeroMQServer extends EventEmitter {
    async listen(port, createReply) {
        this._reqSocket = await this._createSocket(port, (m) => this._onMessage(m, createReply));
        this._pingSocket = await this._createSocket(port + 1, (m) => this._onPingMessage(m));
    }

    _createSocket(port, onMessage) {
        return new Promise((resolve, reject) => {
            const socket = zmq.socket('rep');
            socket.setsockopt(zmq.ZMQ_LINGER, 0);
            socket.on('message', (m) => onMessage(m));
            socket.bind(`tcp://*:${port}`, (err) => {
                if (err) {
                    return reject(err);
                }
                return resolve(socket);
            });
        });
    }

    async _onPingMessage(message) {
        if (message.toString() === connectivity.ping) {
            await this._send(this._pingSocket, connectivity.pong);
        }
    }

    async _onMessage(message, createReply) {
        const reply = createReply(message);
        await this._send(this._reqSocket, reply);
    }

    isServing() {
        if ((this._lastServing) && (Date.now() - this._lastServing < 10000)) {
            return true;
        }
        return false;
    }

    _send(socket, message) {
        return new Promise((resolve, reject) => {
            this._lastServing = Date.now();
            socket.send(message, null, (err, data) => {
                this._lastServing = Date.now();
                if (err) {
                    return reject(err);
                }
                return resolve(data);
            });
        });
    }

    close() {
        this._reqSocket.close();
        this._pingSocket.close();
    }
}

module.exports = { Server: ZeroMQServer };
