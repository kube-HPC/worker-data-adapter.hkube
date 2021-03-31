const log = require('@hkube/logger').GetLogFromContainer();
const { Encoding } = require('@hkube/encoding');
const FifoArray = require('../adapters/zeroMQ/streaming/FifoArray');
const ZMQProducer = require('../adapters/zeroMQ/streaming/ZMQProducer');
const RESPONSE_CACHE = 10;

class MessageProducer {
    constructor(options, consumerNodes, nodeName) {
        this._nodeNames = consumerNodes;
        const { port, encoding, statisticsInterval, messagesMemoryBuff } = options;
        const maxMemorySize = messagesMemoryBuff * 1024 * 1024;
        this._encoding = new Encoding({ type: encoding });
        this._adapter = new ZMQProducer(port, maxMemorySize, (...args) => this._responseAccumulator(...args), this._nodeNames, this._encoding, nodeName);
        this._durationsCache = {};
        this._grossDurationCache = {};
        this._queueTimeCache = {};
        this._responseCount = {};
        this._active = true;
        this._nodeNames.forEach((n) => {
            this._durationsCache[n] = new FifoArray(RESPONSE_CACHE);
            this._grossDurationCache[n] = new FifoArray(RESPONSE_CACHE);
            this._queueTimeCache[n] = new FifoArray(RESPONSE_CACHE);
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

    _responseAccumulator(consumerType, duration, roundTripTime) {
        this._durationsCache[consumerType].append(duration);
        this._grossDurationCache[consumerType].append(roundTripTime);
        this._responseCount[consumerType] += 1;
    }

    _resetDurationsCache(consumerType) {
        const response = this._durationsCache[consumerType].getAsArray();
        this._durationsCache[consumerType].reset();
        return response.slice(0, 10);
    }

    _resetGrossDurationsCache(consumerType) {
        const response = this._grossDurationCache[consumerType].getAsArray();
        this._grossDurationCache[consumerType].reset();
        return response.slice(0, 10);
    }

    _resetQueueDurationsCache(consumerType) {
        const response = this._queueTimeCache[consumerType].getAsArray();
        this._queueTimeCache[consumerType].reset();
        return response.slice(0, 10);
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
                netDurations: this._resetDurationsCache(nodeName),
                durations: this._resetGrossDurationsCache(nodeName),
                queueDurations: this._resetQueueDurationsCache(nodeName),
                responses: this._getResponseCount(nodeName),
                dropped: this._adapter.messageQueue.lostMessages[nodeName]
            };
            statistics.push(singleNodeStatistics);
        });
        this._listeners.forEach((listener) => {
            listener(statistics);
        });
    }

    start() {
        this._adapter.start();
    }

    async close(force = true) {
        if (!this._active) {
            log.info('Attempting to close inactive MessageProducer');
        }
        else {
            this._active = false;
            await this._adapter.close(force);
        }
    }
}

module.exports = MessageProducer;
