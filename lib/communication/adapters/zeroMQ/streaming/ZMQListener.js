const zmq = require('zeromq');
const { uuid } = require('@hkube/uid');
const SIGNALS = require('./signals');
const { waitFor } = require('../../../../utils/waitFor');

const HEARTBEAT_LIVENESS_TIMEOUT = 30000;
const HEARTBEAT_INTERVAL = 10000;
const CHECK_LIVENESS_INTERVAL = 5000;
const INTERVAL_INIT = 1000;
const INTERVAL_MAX = 32000;
const MAX_WAIT_MESSAGES = 5000;

class ZMQListener {
    constructor({ remoteAddress, consumerType, nodeName, encoding, onMessage, onReady, onNotReady }) {
        this._encoding = encoding;
        this._onMessage = onMessage;
        this._onReady = onReady;
        this._onNotReady = onNotReady;
        this._nodeName = nodeName;
        this._consumerType = this._encoding.encode(consumerType, { customEncode: false });
        this._remoteAddress = remoteAddress;
        this._active = true;
        this._worker = null;
        this._lastReceiveTime = null;
        this._lastSentTime = null;
        this._reconnectInterval = INTERVAL_INIT;
        this._emptySignal = Buffer.alloc(1, SIGNALS.PPP_EMPTY);
    }

    start() {
        this._worker = zmq.socket('dealer');
        const identity = this._encoding.encode(uuid(), { customEncode: false });
        this._worker.setsockopt(zmq.ZMQ_IDENTITY, identity);
        this._worker.setsockopt(zmq.ZMQ_LINGER, 0);
        console.log(`zmq listener connecting to ${this._remoteAddress}`);
        this._zmqConnected = false;
        this._worker.removeAllListeners('message');
        this._worker.on('message', this._handleMessage.bind(this));
        this._worker.monitor()
            .on('connect', () => {
                if (!this._zmqConnected) {
                    this._lastMessageTime = Date.now();
                    this._zmqConnected = true;
                    this._reconnectInterval = INTERVAL_INIT;
                    console.log(`zmq listener connected to ${this._remoteAddress}`);
                    this._send(SIGNALS.PPP_INIT);
                }
            })
            .on('disconnect', (error, reason) => {
                if (this._zmqConnected) {
                    console.log(`zmq listener disconnected from ${this._remoteAddress}, ${error}, ${reason}`);
                    this._zmqConnected = false;
                    this._worker.removeAllListeners('message');
                    this._startReconnectTimer();
                }
            });
        this._worker.connect(this._remoteAddress);
        this._checkHeartbeatInterval();
    }

    async close() {
        if (!this._active) {
            console.log('Attempting to close inactive ZMQListener');
        }
        else {
            this._send(SIGNALS.PPP_DISCONNECT);
            clearInterval(this._livenessInterval);
            this._active = false;
            await waitFor(() => !this._lastMessageTime || Date.now() - this._lastMessageTime > MAX_WAIT_MESSAGES);
            this._close();
        }
    }

    ready() {
        if (this._active && !this._readySent) {
            this._readySent = true;
            this._notReadySent = false;
            this._send(SIGNALS.PPP_READY);
        }
    }

    notReady() {
        if (this._active && !this._notReadySent) {
            this._notReadySent = true;
            this._readySent = false;
            this._send(SIGNALS.PPP_NOT_READY);
        }
    }

    _close() {
        this._active = false;
        this._worker?.close();
    }

    async _handleMessage(...frames) {
        this._lastReceiveTime = Date.now();
        this._reconnectInterval = INTERVAL_INIT;
        this._clearReconnectTimer();

        const [signal, flow, header, message] = frames;

        if (signal === SIGNALS.PPP_MSG) {
            this._lastMessageTime = Date.now();
            this._onNotReady(this._remoteAddress);
            const messageFlowPattern = this._encoding.decode(flow, { customEncode: false });
            const result = await this._onMessage(messageFlowPattern, header, message);
            this._onReady(this._remoteAddress);
            let sndSignal = SIGNALS.PPP_DONE;
            if (!this._active) {
                sndSignal = SIGNALS.PPP_DONE_DISCONNECT;
            }
            this._send(sndSignal, result);
        }
    }

    _send(sig, result = this._emptySignal) {
        if (this._worker) {
            const signal = Buffer.alloc(1, sig);
            const message = [signal, this._consumerType, result];
            this._worker.send(message);
            this._lastSentTime = Date.now();
        }
    }

    _checkHeartbeatInterval() {
        this._livenessInterval = setInterval(() => {
            this._checkHeartbeat();
        }, CHECK_LIVENESS_INTERVAL);
    }

    _checkHeartbeat() {
        if (Date.now() - this._lastReceiveTime > HEARTBEAT_LIVENESS_TIMEOUT) {
            // we haven't received any message in too long
            this._startReconnectTimer();
        }
        else if (Date.now() - this._lastSentTime > HEARTBEAT_INTERVAL) {
            // we haven't sent any message in too long
            this._send(SIGNALS.PPP_HEARTBEAT);
        }
    }

    _startReconnectTimer() {
        this._clearReconnectTimer();
        this._reconnectTimerId = setTimeout(this._heartbeatFailure.bind(this), this._reconnectInterval);
    }

    _clearReconnectTimer() {
        if (this._reconnectTimerId) {
            clearTimeout(this._reconnectTimerId);
            this._reconnectTimerId = null;
        }
    }

    _heartbeatFailure() {
        console.log("Heartbeat failure, can't reach queue");
        if (this._reconnectInterval < INTERVAL_MAX) {
            this._reconnectInterval *= 2;
        }
        this._close();
        if (this._active) {
            this.start();
        }
    }
}

module.exports = ZMQListener;
