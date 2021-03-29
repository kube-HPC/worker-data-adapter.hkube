const { Encoding } = require('@hkube/encoding');
const ZMQListener = require('../adapters/zeroMQ/streaming/ZMQListener');

class MessageListener {
    constructor(options, nodeName, errorHandler) {
        this._errorHandler = errorHandler;
        const { encoding, remoteAddress, messageOriginNodeName } = options;
        this._encoding = new Encoding({ type: encoding });
        this._adapter = new ZMQListener({
            remoteAddress,
            consumerType: nodeName,
            nodeName: messageOriginNodeName,
            encoding: this._encoding,
            onMessage: (...args) => this.onMessage(...args),
            onReady: (...args) => this.onReady(...args),
            onNotReady: (...args) => this.onNotReady(...args),
        });
        this._messageOriginNodeName = messageOriginNodeName;
        this._messageListeners = [];
        this._isActive = false;
    }

    registerMessageListener(listener) {
        this._messageListeners.push(listener);
    }

    ready() {
        this._adapter.ready();
    }

    notReady() {
        this._adapter.notReady();
    }

    async onMessage(flowPattern, header, value) {
        const start = Date.now();
        const payload = this._encoding.decodeHeaderPayload(header, value);
        await Promise.all(this._messageListeners.map(async (listener) => {
            try {
                await listener({ flowPattern, payload, origin: this._messageOriginNodeName });
            }
            catch (e) {
                console.log(`Error during MessageListener onMessage ${e.message}`);
            }
        }));
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

    async close() {
        this._isActive = false;
        await this._adapter.close();
        this._messageListeners = [];
    }

    get isActive() {
        return this._isActive;
    }
}

module.exports = MessageListener;
