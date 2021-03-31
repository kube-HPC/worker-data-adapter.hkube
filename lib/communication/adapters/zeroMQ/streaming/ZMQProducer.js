const zmq = require('zeromq');
const log = require('@hkube/logger').GetLogFromContainer();
const Flow = require('./Flow');
const MessageQueue = require('./MessageQueue');
const { waitFor } = require('../../../../utils/waitFor');
const SIGNALS = require('./signals');

class ZMQProducer {
    constructor(port, maxMemorySize, responseAccumulator, consumerTypes, encoding, nodeName) {
        this._nodeName = nodeName;
        this._encoding = encoding;
        this._responseAccumulator = responseAccumulator;
        this._maxMemorySize = maxMemorySize;
        this._consumerTypes = consumerTypes;
        this._READY_SIGNAL = this._createSignal(SIGNALS.PPP_READY);
        this._DONE_SIGNAL = this._createSignal(SIGNALS.PPP_DONE);
        this._messageSignal = Buffer.alloc(1, SIGNALS.PPP_MSG);
        this._noMessageSignal = Buffer.alloc(1, SIGNALS.PPP_NO_MSG);
        this._active = true;
        this._waitingForResponse = new Map();
        this._messageQueue = new MessageQueue(consumerTypes, this._nodeName);
        this._backend = zmq.socket('router'); // ROUTER
        this._backend.bind(`tcp://*:${port}`); // For workers
        log.info(`Producer listening on tcp://*:${port}`);
    }

    _createSignal(signal) {
        return Buffer.alloc(1, signal).toString('utf8');
    }

    get messageQueue() {
        return this._messageQueue;
    }

    produce(header, payload, messageFlowPattern = []) {
        while (this._messageQueue.sizeSum > this._maxMemorySize) {
            this._messageQueue.loseMessage();
        }
        this._messageQueue.append({ messageFlowPattern, header, payload });
    }

    start() {
        this._backend.on('message', async (...args) => {
            const [workerAddress, workerSignal, consumer, result] = args;
            const address = this._encoding.decode(workerAddress, { customEncode: false });
            const signal = workerSignal.toString('utf8');
            const consumerType = this._encoding.decode(consumer, { customEncode: false });

            if (!this._consumerTypes.includes(consumerType)) {
                log.info(`Producer got message from unknown consumer: ${consumerType}, dropping the message (consumers are ${this._consumerTypes.join(',')})`);
                return;
            }
            if (signal === this._DONE_SIGNAL) {
                const sentTime = this._waitingForResponse.get(address);
                if (sentTime) {
                    this._waitingForResponse.delete(address);
                    const roundTripTime = (Date.now() - sentTime);
                    const { duration } = this._encoding.decode(result, { customEncode: false });
                    this._responseAccumulator(consumerType, duration, roundTripTime);
                }
            }
            else if (this._READY_SIGNAL === signal) {
                const message = this._messageQueue.pop(consumerType);
                if (message) {
                    const { messageFlowPattern, header, payload } = message;
                    const flow = new Flow(messageFlowPattern);
                    const flowMsg = this._encoding.encode(flow.getRestOfFlow(this._nodeName), { customEncode: false });
                    const frames = [
                        workerAddress,
                        this._messageSignal,
                        flowMsg,
                        header,
                        payload
                    ];
                    this._waitingForResponse.set(address, Date.now());
                    this._backend.send(frames);
                }
                else {
                    const frames = [
                        workerAddress,
                        this._noMessageSignal,
                    ];
                    this._backend.send(frames);
                }
            }
        });
    }

    queueSize(consumerSize) {
        return this._messageQueue.size(consumerSize);
    }

    sent(consumerType) {
        return this._messageQueue.sent[consumerType];
    }

    async close(force = true) {
        if (!force) {
            await waitFor(() => this._messageQueue.queue.length === 0);
        }
        this._active = false;
        this._backend.close();
    }
}

module.exports = ZMQProducer;