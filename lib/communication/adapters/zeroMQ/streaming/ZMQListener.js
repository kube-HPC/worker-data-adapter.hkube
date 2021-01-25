const zmq = require('zeromq');
const { uuid } = require('@hkube/uid');

const HEARTBEAT_LIVENESS = 5;
const HEARTBEAT_INTERVAL = 1000;
const INTERVAL_INIT = 1000;
const INTERVAL_MAX = 32000;

const PPP_READY = 0x01; // Signals worker is ready
const PPP_HEARTBEAT = 0x02; // Signals worker heartbeat

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
        this._worker.connect(this._remoteAddress);
        console.log(`zmq listener connecting to ${this._remoteAddress}`);
        this._zmqConnected = false;
        this._worker.removeAllListeners('message');
        this._worker.on('message', this._handleMessage.bind(this));
        this._worker.monitor()
            .on('connect', () => {
                if (!this._zmqConnected) {
                    this._send(this._PPP_READY);
                    this._liveness = HEARTBEAT_LIVENESS;
                    this._zmqConnected = true;
                    this._initHeartbeat();
                }
            })
            .on('disconnect', () => {
                if (this._zmqConnected) {
                    this._worker.removeAllListeners('message');
                    this._zmqConnected = false;
                    if (this._reconnectTimerId) {
                        clearTimeout(this._reconnectTimerId);
                    }
                    this._reconnectTimerId = setTimeout(this._heartbeatFailure.bind(this), this._interval);
                    if (this._heartbeatTimerId !== -1) {
                        clearTimeout(this._heartbeatTimerId);
                        this._heartbeatTimerId = -1;
                    }
                }
            });
    }

    close() {
        if (this._active) {
            console.log('Attempting to close inactive ZMQListener');
        }
        else {
            this._active = false;
        }
    }

    async _handleMessage(...args) {
        if (this._reconnectTimerId !== -1) {
            // a heartbeat has arrived whilst we were preparing to destroy and recreate the socket. cancel that
            clearTimeout(this._reconnectTimerId);
            this._reconnectTimerId = -1;
        }
        if (this._heartbeatTimerId === -1) {
            // for some reason we've received a heartbeat or message when the heartbeat timer wasn't set. schedule it again
            this._initHeartbeat();
        }

        this._liveness = HEARTBEAT_LIVENESS;
        if (args.length === 1 && args[0].toString('utf8') === this._PPP_HEARTBEAT.toString('utf8')) {
            // a heartbeat has arrived
        }
        else if (args.length === 3) {
            const [flow, header, message] = args;
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
        if (this._heartbeatTimerId !== -1) {
            clearTimeout(this._heartbeatTimerId);
        }
        this._heartbeatTimerId = setTimeout(this._checkHeartbeat.bind(this), HEARTBEAT_INTERVAL);
        if (this._reconnectTimerId !== -1) {
            clearTimeout(this._reconnectTimerId);
            this._reconnectTimerId = -1;
        }
    }

    _checkHeartbeat() {
        if (--this._liveness === 0) {
            // we haven't received a heartbeat in too long
            this._reconnectTimerId = setTimeout(this._heartbeatFailure.bind(this), this._interval);
            if (this.heartbeatTimerId) {
                clearTimeout(this._heartbeatTimerId);
                this._heartbeatTimerId = -1;
            }
        }
        else {
            this._send(this._PPP_HEARTBEAT);
            this._initHeartbeat();
        }
    }

    _heartbeatFailure() {
        console.log("Heartbeat failure, can't reach queue");
        if (this._interval < this._intervalMax) {
            this._interval *= 2;
        }
        this._worker.close();
        this._worker = null;
        if (this._active) {
            this.start();
        }
    }
}

module.exports = ZMQListener;
