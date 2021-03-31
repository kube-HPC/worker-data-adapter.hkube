const log = require('@hkube/logger').GetLogFromContainer();
const MessageListener = require('./MessageListener');
const MessageProducer = require('./MessageProducer');
const { sleep } = require('../../utils/waitFor');

class StreamingManager {
    constructor(errorHandler) {
        this._errorHandler = errorHandler;
        this._messageProducer = null;
        this._messageListeners = {};
        this._inputListener = [];
        this._listeningToMessages = false;
        this._parsedFlows = {};
        this._defaultFlow = null;
    }

    sendError(e) {
        this._errorHandler(e);
    }

    setupStreamingProducer({ onStatistics, producerConfig, nextNodes, nodeName, parsedFlow, defaultFlow }) {
        this._parsedFlows = parsedFlow;
        this._defaultFlow = defaultFlow;
        this._messageProducer = new MessageProducer(producerConfig, nextNodes, nodeName);
        this._messageProducer.registerStatisticsListener(onStatistics);
        if (nextNodes) {
            this._messageProducer.start();
        }
    }

    setupStreamingListeners(listenerConfig, parents, nodeName) {
        parents.forEach((parent) => {
            const { address, type } = parent;
            const remoteAddress = `tcp://${address.host}:${address.port}`;

            if (type === 'Add') {
                const options = {
                    ...listenerConfig,
                    remoteAddress,
                    messageOriginNodeName: parent.nodeName,
                };

                const listener = new MessageListener(options, nodeName, (e) => this.sendError(e));
                listener.registerMessageListener((...args) => this._onMessage(...args));
                this._messageListeners[remoteAddress] = listener;
            }
            if (type === 'Del') {
                this._messageListeners[remoteAddress]?.close(false);
                delete this._messageListeners[remoteAddress];
            }
        });
    }

    registerInputListener(onMessage) {
        this._inputListener.push(onMessage);
    }

    async _onMessage({ flowPattern, payload, origin }) {
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
            }
            if (!parsedFlow) {
                throw new Error(`No such flow ${flow}`);
            }
            this._messageProducer.produce(parsedFlow, message);
        }
    }

    async stopStreaming(force = true) {
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

    get listeningToMessages() {
        return this._listeningToMessages;
    }
}

module.exports = StreamingManager;