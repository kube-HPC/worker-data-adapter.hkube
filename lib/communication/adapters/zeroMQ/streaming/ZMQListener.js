const zmq = require('zeromq');
const { uuid } = require('@hkube/uid');
const { waitFor } = require('../../../../utils/waitFor');

const HEARTBEAT_LIVENESS = 5;
const HEARTBEAT_INTERVAL = 1000;
const INTERVAL_INIT = 1000;
const INTERVAL_MAX = 32000;
const PPP_READY = 0x01; // Signals worker is ready
const PPP_HEARTBEAT = 0x02; // Signals worker heartbeat
const MAX_WAIT_MESSAGES = 5000;

class ZMQListener {
    constructor(remoteAddress, onMessage, encoding, consumerType) {
        this._encoding = encoding;
        this._onMessage = onMessage;
        this._consumerType = this._encoding.encode(consumerType, { customEncode: false });
        this._remoteAddress = remoteAddress;
        this._active = true;
        this._worker = null;
        this._PPP_READY = Buffer.alloc(1, PPP_READY);
        this._PPP_HEARTBEAT = Buffer.alloc(1, PPP_HEARTBEAT);
        this._interval = INTERVAL_INIT;
        this._intervalInit = INTERVAL_INIT;
        this._intervalMax = INTERVAL_MAX;
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
                    console.log(`zmq listener connected to ${this._remoteAddress}`);
                    this._send(this._PPP_READY);
                    this._liveness = HEARTBEAT_LIVENESS;
                    this._zmqConnected = true;
                    this._initHeartbeat();
                }
            })
            .on('disconnect', (error, reason) => {
                if (this._zmqConnected) {
                    console.log(`zmq listener disconnected from ${this._remoteAddress}, ${error}, ${reason}`);
                    this._worker.removeAllListeners('message');
                    this._zmqConnected = false;
                    this._clearReconnectTimer();
                    this._reconnectTimerId = setTimeout(this._heartbeatFailure.bind(this), this._interval);
                    this._clearHeartbeatTimer();
                }
            });
        this._worker.connect(this._remoteAddress);
    }

    async close() {
        if (!this._active) {
            console.log('Attempting to close inactive ZMQListener');
        }
        else {
            this._active = false;
            await waitFor(() => !this._lastMessageTime || Date.now() - this._lastMessageTime > MAX_WAIT_MESSAGES);
            this._close();
        }
    }

    _close() {
        this._active = false;
        this._worker?.close();
        this._worker = null;
    }

    async _handleMessage(...frames) {
        this._clearReconnectTimer();
        if (!this._heartbeatTimerId) {
            // for some reason we've received a heartbeat or message when the heartbeat timer wasn't set. schedule it again
            this._initHeartbeat();
        }
        this._liveness = HEARTBEAT_LIVENESS;
        if (frames.length === 1 && frames[0].toString('utf8') === this._PPP_HEARTBEAT.toString('utf8')) {
            // a heartbeat has arrived
        }
        else if (frames.length === 3) {
            this._lastMessageTime = Date.now();
            const [flow, header, message] = frames;
            const messageFlowPattern = this._encoding.decode(flow, { customEncode: false });
            const result = await this._onMessage(messageFlowPattern, header, message);
            try {
                this._send(result);
            }
            catch (e) {
                if (this._active) {
                    throw e;
                }
            }
        }
        else {
            console.log('Invalid message frames');
        }
        this._interval = this._intervalInit;
    }

    _send(data) {
        if (this._active) {
            this._worker.send([data, this._consumerType]);
        }
    }

    _initHeartbeat() {
        this._clearHeartbeatTimer();
        this._heartbeatTimerId = setTimeout(this._checkHeartbeat.bind(this), HEARTBEAT_INTERVAL);
        this._clearReconnectTimer();
    }

    _checkHeartbeat() {
        this._liveness -= 1;
        if (this._liveness === 0) {
            // we haven't received a heartbeat in too long
            this._reconnectTimerId = setTimeout(this._heartbeatFailure.bind(this), this._interval);
            this._clearHeartbeatTimer();
        }
        else {
            this._send(this._PPP_HEARTBEAT);
            this._initHeartbeat();
        }
    }

    _clearHeartbeatTimer() {
        if (this._heartbeatTimerId) {
            clearTimeout(this._heartbeatTimerId);
            this._heartbeatTimerId = null;
        }
    }

    _clearReconnectTimer() {
        if (this._reconnectTimerId) {
            clearTimeout(this._reconnectTimerId);
            this._reconnectTimerId = null;
        }
    }

    _heartbeatFailure() {
        console.log("Heartbeat failure, can't reach queue");
        if (this._interval < this._intervalMax) {
            this._interval *= 2;
        }
        this._close();
        if (this._active) {
            this.start();
        }
    }
}

module.exports = ZMQListener;
