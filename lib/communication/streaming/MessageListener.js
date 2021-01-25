const { Encoding } = require('@hkube/encoding');
const ZMQListener = require('../adapters/zeroMQ/streaming/ZMQListener');

class MessageListener {
    constructor(options, nodeName, errorHandler) {
        this._errorHandler = errorHandler;
        const { encoding, remoteAddress, messageOriginNodeName } = options;
        this._encoding = new Encoding({ type: encoding });
        this._adapter = new ZMQListener(remoteAddress, (...args) => this.onMessage(...args), this._encoding, nodeName);
        this._messageOriginNodeName = messageOriginNodeName;
        this._messageListeners = [];
        this._isActive = false;
    }

    registerMessageListener(listener) {
        this._messageListeners.push(listener);
    }

    onMessage(messageFlowPattern, header, value) {
        const start = Date.now();
        const payload = this._encoding.decodeHeaderPayload(header, value);
        this._messageListeners.forEach((listener) => {
            try {
                listener({ messageFlowPattern, payload, origin: this._messageOriginNodeName });
            }
            catch (e) {
                console.log(`Error during MessageListener onMessage ${e.message}`);
            }
        });
        const end = Date.now();
        const duration = (end - start);
        return this._encoding.encode({ duration }, { customEncode: false });
    }

    start() {
        this._isActive = true;
        console.log(`start receiving from ${this._messageOriginNodeName}`);
        try {
            this._adapter.start();
        }
        catch (e) {
            this._isActive = false;
            if (this._errorHandler) {
                this._errorHandler(e);
            }
        }
    }

    close() {
        this._isActive = false;
        this.messageListeners = [];
        this.adapter.close();
    }

    get isActive() {
        return this._isActive;
    }
}

module.exports = MessageListener;
