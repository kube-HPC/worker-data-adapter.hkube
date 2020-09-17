const zmq = require('zeromq');
const EventEmitter = require('events');
const { requestType, connectivity } = require('../../../consts/messages');
const RequestError = require('./request-error');

class ZeroMQRequest extends EventEmitter {
    constructor({ address, content, timeout, networkTimeout }) {
        super();
        this.content = content;
        this.port = address.port;
        this.host = address.host;
        this.timeout = timeout;
        this.networkTimeout = networkTimeout;
        this.socket = zmq.socket('req');
        this.socket.setsockopt('linger', 0);
        this.socket.connect(this.address);
    }

    get address() {
        return `tcp://${this.host}:${this.port}`;
    }

    async invoke() {
        const pongRes = await this._send({ type: requestType.ping, content: connectivity.ping, timeout: this.networkTimeout });
        let result = null;
        if (pongRes.length === 1 && pongRes[0].toString() === connectivity.pong) {
            result = await this._send({ type: requestType.request, content: this.content, timeout: this.timeout });
        }
        return result;
    }

    _send({ type, content, timeout }) {
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.socket.removeAllListeners('message');
                return reject(new RequestError(type.errorCode, `${type.errorCode} (${timeout}) to ${this.address}`));
            }, timeout);
            this.socket.on('message', (...parts) => {
                clearTimeout(timer);
                this.socket.removeAllListeners('message');
                return resolve(parts);
            });
            this.socket.send(content);
        });
    }

    close() {
        this.socket.close();
    }
}
module.exports = { Request: ZeroMQRequest };
