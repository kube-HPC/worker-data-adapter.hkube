const MessageListener = require('./MessageListener');
const MessageProducer = require('./MessageProducer');

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
        console.log(`parents ${JSON.stringify(parents, null, 2)}`);
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
                if (this._listeningToMessages) {
                    listener.start();
                }
            }
            if (type === 'Del') {
                if (this._listeningToMessages) {
                    this._messageListeners[remoteAddress].close();
                    delete this._messageListeners[remoteAddress];
                }
            }
        });
    }

    registerInputListener(onMessage) {
        this._inputListener.push(onMessage);
    }

    _onMessage({ messageFlowPattern, payload, origin }) {
        // this._messageFlowPattern = messageFlowPattern;
        this._inputListener.forEach((listener) => {
            try {
                listener(payload, origin);
            }
            catch (e) {
                console.log(`hkube_api message listener through exception: ${e.message}`);
            }
        });
        // this._messageFlowPattern = [];
    }

    startMessageListening() {
        this._listeningToMessages = true;
        Object.values(this._messageListeners).forEach((listener) => {
            if (!listener.isActive) {
                listener.start();
            }
        });
    }

    sendMessage({ message, flow }) {
        if (!this._messageProducer) {
            throw new Error('Trying to send a message from a none stream pipeline or after close had been applied on algorithm');
        }
        if (this._messageProducer.nodeNames?.length) {
            let parsedFlow = null;
            let flowName = flow;
            if (!flowName) {
                if (this._messageFlowPattern?.length) {
                    parsedFlow = this._messageFlowPattern;
                }
                else {
                    if (!this._defaultFlow) {
                        throw new Error('Streaming default flow is null');
                    }
                    flowName = this._defaultFlow;
                }
            }
            if (!parsedFlow) {
                parsedFlow = this._parsedFlows[flowName];
            }
            if (!parsedFlow) {
                throw new Error(`No such flow ${flowName}`);
            }
            this._messageProducer.produce(parsedFlow, message);
        }
    }

    stopStreaming(force = true) {
        if (this._listeningToMessages) {
            Object.values(this._messageListeners).forEach((listener) => {
                listener.close();
            });
            this._messageListeners = {};
        }
        this._listeningToMessages = false;
        this._inputListener = [];
        if (this._messageProducer) {
            this._messageProducer.close(force);
            this._messageProducer = null;
        }
    }
}

module.exports = StreamingManager;
