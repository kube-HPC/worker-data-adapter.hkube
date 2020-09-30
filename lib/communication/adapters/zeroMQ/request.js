const zmq = require('zeromq');
const EventEmitter = require('events');
const { requestType, connectivity } = require('../../../consts/messages');
const RequestError = require('./request-error');
const timing = require('../../../utils/timing');

class ZeroMQRequest extends EventEmitter {
    constructor({ address, content, timeout, networkTimeout }) {
        super();
        this._content = content;
        this._port = parseInt(address.port, 10);
        this._host = address.host;
        this._timeout = timeout;
        this._networkTimeout = networkTimeout;
        this._reqSocket = this._createSocket(this._host, this._port);
        this._pingSocket = this._createSocket(this._host, this._port + 1);
        this._sendPing = timing(this._sendPing.bind(this));
        this._sendMsg = timing(this._sendMsg.bind(this));
    }

    get address() {
        return `tcp://${this._host}:${this._port}`;
    }

    async invoke() {
        console.log('sending ping');
        const pongRes = await this._sendPing(requestType.ping);
        let result = null;
        if (pongRes.length === 1 && pongRes[0].toString() === connectivity.pong) {
            console.log('got pong');
            result = await this._sendMsg(requestType.request);
        }
        return result;
    }

    async _sendPing(type) {
        return this._send({
            type,
            address: this._pingSocket.address,
            socket: this._pingSocket.socket,
            content: connectivity.ping,
            timeout: this._networkTimeout
        });
    }

    async _sendMsg(type) {
        return this._send({
            type,
            address: this._reqSocket.address,
            socket: this._reqSocket.socket,
            content: this._content,
            timeout: this._timeout
        });
    }

    _send({ type, address, socket, content, timeout }) {
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                socket.removeAllListeners('message');
                return reject(new RequestError(type.errorCode, `${type.errorCode} (${timeout}) to ${address}`));
            }, timeout);
            socket.on('message', (...parts) => {
                clearTimeout(timer);
                socket.removeAllListeners('message');
                return resolve(parts);
            });
            socket.send(content);
        });
    }

    _createSocket(host, port) {
        const address = `tcp://${host}:${port}`;
        const socket = zmq.socket('req');
        socket.setsockopt(zmq.ZMQ_LINGER, 0);
        socket.connect(address);
        return { address, socket };
    }

    close() {
        this._reqSocket.socket.close();
        this._pingSocket.socket.close();
    }
}
module.exports = { Request: ZeroMQRequest };
