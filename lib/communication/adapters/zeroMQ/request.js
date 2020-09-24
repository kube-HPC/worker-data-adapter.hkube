const zmq = require('zeromq');
const EventEmitter = require('events');
const { requestType, connectivity } = require('../../../consts/messages');
const RequestError = require('./request-error');

class ZeroMQRequest extends EventEmitter {
    constructor({ address, content, timeout, networkTimeout }) {
        super();
        this._content = content;
        this._port = address.port;
        this._host = address.host;
        this._timeout = timeout;
        this._networkTimeout = networkTimeout;
        this._socket = zmq.socket('req');
        this._socket.setsockopt(zmq.ZMQ_LINGER, 0);
        this._socket.connect(this.address);
    }

    get address() {
        return `tcp://${this._host}:${this._port}`;
    }

    async invoke() {
        const pongRes = await this._send({ type: requestType.ping, content: connectivity.ping, timeout: this._networkTimeout });
        let result = null;
        if (pongRes.length === 1 && pongRes[0].toString() === connectivity.pong) {
            result = await this._send({ type: requestType.request, content: this._content, timeout: this._timeout });
        }
        return result;
    }

    _send({ type, content, timeout }) {
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this._socket.removeAllListeners('message');
                return reject(new RequestError(type.errorCode, `${type.errorCode} (${timeout}) to ${this.address}`));
            }, timeout);
            this._socket.on('message', (...parts) => {
                clearTimeout(timer);
                this._socket.removeAllListeners('message');
                return resolve(parts);
            });
            this._socket.send(content);
        });
    }

    close() {
        this._socket.close();
    }
}
module.exports = { Request: ZeroMQRequest };
