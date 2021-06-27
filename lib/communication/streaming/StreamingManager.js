const log = require('@hkube/logger').GetLogFromContainer();
const MessageListener = require('./MessageListener');
const MessageProducer = require('./MessageProducer');
const { sleep } = require('../../utils/waitFor');

class StreamingManager {
    constructor() {
        this._messageProducer = null;
        this._messageListeners = {};
        this._inputListener = [];
        this._listeningToMessages = false;
        this._parsedFlows = {};
        this._defaultFlow = null;
        this._isStopping = false;
    }

    setupStreamingProducer({ onStatistics, producerConfig, consumers, nodeName, parsedFlow, defaultFlow }) {
        this._parsedFlows = parsedFlow;
        this._defaultFlow = defaultFlow;
        this._messageProducer = new MessageProducer({
            options: producerConfig,
            consumers,
            nodeName
        });
        this._messageProducer.registerStatisticsListener(onStatistics);
        this._messageProducer.start();
    }

    setupStreamingListeners({ listenerConfig, discovery, consumerName }) {
        if (this._isStopping) {
            return;
        }
        discovery.forEach(async (d) => {
            const { address, type } = d;
            const remoteAddress = `tcp://${address.host}:${address.port}`;

            if (type === 'Add') {
                const options = {
                    ...listenerConfig,
                    remoteAddress,
                    messageOriginNodeName: d.nodeName,
                };
                const listener = new MessageListener({ options, consumerName });
                listener.registerMessageListener((...args) => this._onMessage(...args));
                this._messageListeners[remoteAddress] = listener;
            }
            if (type === 'Del') {
                const listener = this._messageListeners[remoteAddress];
                if (listener) {
                    await listener.close(false);
                    delete this._messageListeners[remoteAddress];
                }
            }
        });
    }

    registerInputListener(onMessage) {
        this._inputListener.push(onMessage);
    }

    async _onMessage({ flowPattern, payload, origin }) {
        if (this._inputListener.length === 0) {
            log.error('no input listeners found on message');
            return;
        }
        await Promise.all(this._inputListener.map(async (listener) => {
            try {
                const sendMessage = (message) => {
                    this.sendMessage({ message, flowPattern });
                };
                await listener({ payload, origin, sendMessage });
            }
            catch (e) {
                log.error(`hkube_api message listener error: ${e.message}`);
            }
        }));
    }

    async startMessageListening() {
        this._listeningToMessages = true;
        while (this._listeningToMessages) {
            const listeners = Object.values(this._messageListeners);
            if (listeners.length === 0) {
                await sleep(200);
            }
            for (const listener of listeners) {  // eslint-disable-line
                await listener.fetch();
            }
        }
    }

    sendMessage({ message, flowName, flowPattern }) {
        if (!this._messageProducer) {
            throw new Error('Trying to send a message from a none stream pipeline or after close had been applied on algorithm');
        }
        if (this._messageProducer.nodeNames?.length) {
            let parsedFlow = null;
            let flow = flowName;
            if (!flow) {
                if (flowPattern) {
                    parsedFlow = flowPattern;
                }
                else {
                    if (!this._defaultFlow) {
                        throw new Error('Streaming default flow is null');
                    }
                    flow = this._defaultFlow;
                }
            }
            if (!parsedFlow) {
                parsedFlow = this._parsedFlows[flow];
                if (!parsedFlow) {
                    throw new Error(`No such flow ${flow}`);
                }
            }
            this._messageProducer.produce(parsedFlow, message);
        }
    }

    startStreaming() {
        this._isStopping = false;
    }

    async stopStreaming(force = true) {
        if (this._isStopping) {
            return;
        }
        try {
            this._isStopping = true;
            if (this._listeningToMessages) {
                await Promise.all(Object.values(this._messageListeners).map((listener) => listener.close(force)));
                this._messageListeners = {};
                this._listeningToMessages = false;
                this._inputListener = [];
            }
            if (this._messageProducer) {
                await this._messageProducer.close(force);
                this._messageProducer = null;
            }
        }
        finally {
            this._isStopping = false;
        }
    }

    get listeningToMessages() {
        return this._listeningToMessages;
    }
}

module.exports = StreamingManager;
