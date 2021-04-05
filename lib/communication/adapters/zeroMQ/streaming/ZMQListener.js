const zmq = require('zeromq');
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
        this._messages = [];
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
            this._send(this._readySignal);
            const hasFrames = await this._waitForMsg();
            await this._handleMessage(hasFrames);
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

    async _handleMessage(hasFrames) {
        if (!hasFrames) {
            return;
        }
        const [sig, flow, header, message] = this._messages.shift();
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
        await waitFor(() => !this._working && !force);
        this._worker?.close();
    }

    _waitForMsg() {
        if (this._messages.length) {
            return true;
        }
        return new Promise((resolve) => {
            this._timer = setTimeout(() => {
                clearTimeout(this._timer);
                clearInterval(this._interval);
                this._timer = null;
                this._interval = null;
                resolve(false);
            }, POLL_MS);

            this._interval = setInterval(() => {
                if (this._messages.length) {
                    clearTimeout(this._timer);
                    clearInterval(this._interval);
                    this._timer = null;
                    this._interval = null;
                    resolve(true);
                }
            }, 1);
        });
    }

    _onSocketMessage(...frames) {
        this._messages.push(frames);
    }

    _send(signal, result = this._emptySignal) {
        const frames = [signal, this._consumerType, result];
        this._worker.send(frames);
    }
}

module.exports = ZMQListener;
