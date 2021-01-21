const { Encoding } = require('@hkube/encoding');
const FifoArray = require('../adapters/zeroMQ/streaming/FifoArray');
const ZMQProducer = require('../adapters/zeroMQ/streaming/ZMQProducer');
const RESPONSE_CACHE = 2000;

class MessageProducer {
    constructor(options, consumerNodes, nodeName) {
        this._nodeNames = consumerNodes;
        const { port, encoding, statisticsInterval, messagesMemoryBuff } = options;
        const maxMemorySize = messagesMemoryBuff * 1024 * 1024;
        this._encoding = new Encoding({ type: encoding });
        this._adapter = new ZMQProducer(port, maxMemorySize, (...args) => this._responseAccumulator(...args), this._nodeNames, this._encoding, nodeName);
        this._responsesCache = {};
        this._responseCount = {};
        this._active = true;
        this._printStatistics = 0;
        this._nodeNames.forEach((n) => {
            this._responsesCache[n] = new FifoArray(RESPONSE_CACHE);
            this._responseCount[n] = 0;
        });
        this._listeners = [];
        this._sendStatistics(statisticsInterval);
    }

    get nodeNames() {
        return this._nodeNames;
    }

    _sendStatistics(interval) {
        if (this._nodeNames) {
            const statisticsInterval = setInterval(() => {
                if (this._active) {
                    this._sendStatisticsData();
                }
                else {
                    clearInterval(statisticsInterval);
                }
            }, interval);
        }
    }

    produce(messageFlowPattern, obj) {
        const { header, payload } = this._encoding.encodeHeaderPayload(obj);
        this._adapter.produce(header, payload, messageFlowPattern);
    }

    _responseAccumulator(response, consumerType) {
        const decodedResponse = this._encoding.decode(response, { customEncode: false });
        this._responseCount[consumerType] += 1;
        const { duration } = decodedResponse;
        this._responsesCache[consumerType].push(duration);
    }

    _resetResponseCache(consumerType) {
        const responsePerNode = this._responsesCache[consumerType].getAsArray();
        this._responsesCache[consumerType].reset();
        return responsePerNode.slice(0, 10);
    }

    _getResponseCount(consumerType) {
        return this._responseCount[consumerType];
    }

    registerStatisticsListener(listener) {
        this._listeners.push(listener);
    }

    _sendStatisticsData() {
        const statistics = [];
        this._nodeNames.forEach((nodeName) => {
            const queueSize = this._adapter.queueSize(nodeName);
            const sent = this._adapter.sent(nodeName);
            const singleNodeStatistics = {
                nodeName,
                sent,
                queueSize,
                durations: this._resetResponseCache(nodeName),
                responses: this._getResponseCount(nodeName),
                dropped: this._adapter.messageQueue.lostMessages[nodeName]
            };
            statistics.push(singleNodeStatistics);
        });
        this._listeners.forEach((listener) => {
            listener(statistics);
        });
        if (this._printStatistics % 10 === 0) {
            console.log(`statistics ${statistics}`);
        }
        this._printStatistics += 1;
    }

    start() {
        this._adapter.start();
    }

    close(force = true) {
        if (!this._active) {
            console.log('Attempting to close inactive MessageProducer');
        }
        else {
            this._active = false;
            this._adapter.close(force);
        }
    }
}

module.exports = MessageProducer;
