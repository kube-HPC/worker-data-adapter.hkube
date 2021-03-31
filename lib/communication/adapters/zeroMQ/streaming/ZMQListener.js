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
        this._timeoutCount = 0;
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
        const identity = this._encoding.encode(uuid(), { customEncode: false });
        worker.setsockopt(zmq.ZMQ_IDENTITY, identity);
        worker.setsockopt(zmq.ZMQ_LINGER, 0);
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

            if (this._timeoutCount === 3) {
                this._timeoutCount = 0;
                this._worker.close();
                this._worker = this._workerSocket();
                return;
            }

            if (this._timeoutCount > 0) {
                const frames = await this._waitForMsg();
                this._handleFrames(frames);
                return;
            }
            this._send(this._readySignal);
            const frames = await this._waitForMsg();
            this._handleFrames(frames);
        }
        catch (error) {

        }
        finally {
            if (!this._active) {
                this._working = false;
            }
        }
    }

    async _handleFrames(frames) {
        if (frames) {
            this._timeoutCount = 0;
            await this._handleMessage(frames);
        }
        else {
            this._timeoutCount += 1;
        }
    }

    async _handleMessage(frames) {
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
        if (!this._active) {
            log.info('Attempting to close inactive ZMQListener');
        }
        else {
            this._active = false;
            await waitFor(() => !this._working && !force);
            this._worker?.close();
        }
    }

    _waitForMsg() {
        return new Promise((resolve) => {
            this._timer = setTimeout(() => {
                clearTimeout(this._timer);
                clearInterval(this._interval);
                this._timer = null;
                this._interval = null;
                return resolve();
            }, POLL_MS);

            this._interval = setInterval(() => {
                if (this._frames) {
                    clearTimeout(this._timer);
                    clearInterval(this._interval);
                    this._timer = null;
                    this._interval = null;
                    resolve(this._frames);
                    this._frames = null;
                }
            }, 1);
        });
    }

    _onSocketMessage(...frames) {
        if (this._frames) {
            log.error('got frames while already has');
        }
        this._frames = frames;
    }

    _send(signal, result = this._emptySignal) {
        const frames = [signal, this._consumerType, result];
        this._worker.send(frames);
    }
}

module.exports = ZMQListener;
