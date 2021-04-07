const zmq = require('zeromq');
const EventEmitter = require('events');
const log = require('@hkube/logger').GetLogFromContainer();
const { uuid } = require('@hkube/uid');
const SIGNALS = require('./signals');
const { waitFor, sleep } = require('../../../../utils/waitFor');
const POLL_MS = 1000;

class ZMQListener {
    constructor({ remoteAddress, consumerType, nodeName, encoding, onMessage }) {
        this._encoding = encoding;
        this._onMessage = onMessage;
        this._nodeName = nodeName;
        this._consumerType = this._encoding.encode(consumerType, { customEncode: false });
        this._remoteAddress = remoteAddress;
        this._active = true;
        this._working = true;
        this._events = new EventEmitter();
        this._readySignal = Buffer.alloc(1, SIGNALS.PPP_READY);
        this._doneSignal = Buffer.alloc(1, SIGNALS.PPP_DONE);
        this._emptySignal = Buffer.alloc(1, SIGNALS.PPP_EMPTY);
        this._messageSignal = Buffer.alloc(1, SIGNALS.PPP_MSG).toString('utf8');
        this._worker = this._workerSocket();
    }

    _workerSocket() {
        if (this._worker) {
            this._worker.removeAllListeners('message');
        }
        const worker = zmq.socket('dealer');
        const identity = Buffer.from(uuid());
        worker.setsockopt(zmq.ZMQ_IDENTITY, identity);
        log.info(`zmq listener connecting to ${this._remoteAddress}`);
        worker.on('message', this._onSocketMessage.bind(this));
        worker.connect(this._remoteAddress);
        return worker;
    }

    async fetch() {
        try {
            if (!this._active) {
                await sleep(200);
                return;
            }
            if (!this._waitingForMessage) {
                this._waitingForMessage = true;
                this._send(this._readySignal);
            }
            const frames = await this._waitForMsg();
            await this._handleMessage(frames);
        }
        catch (e) {
            log.throttle.error(e.message);
        }
        finally {
            if (!this._active) {
                this._working = false;
            }
        }
    }

    async _handleMessage(frames) {
        this._frames = null;
        this._waitingForMessage = false;
        const [sig, flow, header, message] = frames;
        const signal = sig.toString('utf8');
        if (signal === this._messageSignal) {
            const messageFlowPattern = this._encoding.decode(flow, { customEncode: false });
            const result = await this._onMessage(messageFlowPattern, header, message);
            this._send(this._doneSignal, result);
        }
        else {
            await sleep(5);
        }
    }

    async close(force = true) {
        this._active = false;
        if (!force) {
            await waitFor(() => !this._working);
        }
        this._worker?.close();
    }

    _waitForMsg() {
        if (this._frames) {
            return this._frames;
        }
        return new Promise((resolve) => {
            this._events.removeAllListeners();
            this._events.on('frames', (frames) => {
                clearTimeout(this._timer);
                this._timer = null;
                resolve(frames);
            });
            this._timer = setTimeout(() => {
                resolve(false);
            }, POLL_MS);
        });
    }

    _onSocketMessage(...frames) {
        this._frames = frames;
        this._events.emit('frames', frames);
    }

    _send(signal, result = this._emptySignal) {
        const frames = [signal, this._consumerType, result];
        this._worker.send(frames);
    }
}

module.exports = ZMQListener;
