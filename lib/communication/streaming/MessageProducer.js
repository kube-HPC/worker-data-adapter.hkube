const { Encoding } = require('@hkube/encoding');
const FifoArray = require('../adapters/zeroMQ/streaming/FifoArray');
const ZMQProducer = require('../adapters/zeroMQ/streaming/ZMQProducer');
const RESPONSE_CACHE = 10;

class MessageProducer {
    constructor({ options, consumers, nodeName }) {
        this._consumers = consumers;
        const { port, encoding, statisticsInterval, messagesMemoryBuff } = options;
        const maxMemorySize = messagesMemoryBuff * 1024 * 1024;
        this._encoding = new Encoding({ type: encoding });
        this._adapter = new ZMQProducer({
            port,
            maxMemorySize,
            responseAccumulator: (...args) => this._responseAccumulator(...args),
            consumerTypes: this._consumers,
            encoding: this._encoding,
            nodeName
        });
        this._durationsCache = {};
        this._grossDurationCache = {};
        this._queueTimeCache = {};
        this._responseCount = {};
        this._active = true;
        this._consumers.forEach((n) => {
            this._durationsCache[n] = new FifoArray(RESPONSE_CACHE);
            this._grossDurationCache[n] = new FifoArray(RESPONSE_CACHE);
            this._queueTimeCache[n] = new FifoArray(RESPONSE_CACHE);
            this._responseCount[n] = 0;
        });
        this._listeners = [];
        this._sendStatistics(statisticsInterval);
    }

    get nodeNames() {
        return this._consumers;
    }

    _sendStatistics(interval) {
        if (this._consumers) {
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

    produce(flow, obj) {
        const { header, payload } = this._encoding.encodeHeaderPayload(obj);
        this._adapter.produce(header, payload, flow);
    }

    _responseAccumulator(consumerType, duration, roundTripTime) {
        this._durationsCache[consumerType].append(duration);
        this._grossDurationCache[consumerType].append(roundTripTime);
        this._responseCount[consumerType] += 1;
    }

    _resetDurationsCache(consumerType) {
        const response = this._durationsCache[consumerType].getAsArray();
        this._durationsCache[consumerType].reset();
        return response;
    }

    _resetGrossDurationsCache(consumerType) {
        const response = this._grossDurationCache[consumerType].getAsArray();
        this._grossDurationCache[consumerType].reset();
        return response;
    }

    _resetQueueDurationsCache(consumerType) {
        const response = this._queueTimeCache[consumerType].getAsArray();
        this._queueTimeCache[consumerType].reset();
        return response.slice;
    }

    _getResponseCount(consumerType) {
        return this._responseCount[consumerType];
    }

    registerStatisticsListener(listener) {
        this._listeners.push(listener);
    }

    _sendStatisticsData() {
        const statistics = [];
        this._consumers.forEach((nodeName) => {
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
        if (this._active) {
            this._active = false;
            await this._adapter.close(force);
        }
    }
}

module.exports = MessageProducer;
