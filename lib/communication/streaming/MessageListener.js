const log = require('@hkube/logger').GetLogFromContainer();
const { Encoding } = require('@hkube/encoding');
const ZMQListener = require('../adapters/zeroMQ/streaming/ZMQListener');

class MessageListener {
    constructor({ options, consumerName }) {
        const { encoding, remoteAddress, messageOriginNodeName } = options;
        this._encoding = new Encoding({ type: encoding });
        this._adapter = new ZMQListener({
            remoteAddress,
            consumerName,
            encoding: this._encoding,
            onMessage: (...args) => this.onMessage(...args)
        });
        this._messageOriginNodeName = messageOriginNodeName;
        this._messageListeners = [];
        this._isActive = true;
    }

    registerMessageListener(listener) {
        this._messageListeners.push(listener);
    }

    async onMessage(flowPattern, header, value) {
        const start = Date.now();
        const payload = this._encoding.decodeHeaderPayload(header, value);
        await Promise.all(this._messageListeners.map(async (listener) => {
            try {
                await listener({ flowPattern, payload, origin: this._messageOriginNodeName });
            }
            catch (e) {
                log.error(`Error during MessageListener onMessage ${e.message}`);
            }
        }));
        const end = Date.now();
        const duration = (end - start);
        return this._encoding.encode({ duration }, { customEncode: false });
    }

    async fetch() {
        await this._adapter.fetch();
    }

    async close(force = true) {
        if (this._isActive) {
            this._isActive = false;
            await this._adapter.close(force);
            this._messageListeners = [];
        }
    }

    get isActive() {
        return this._isActive;
    }
}

module.exports = MessageListener;
