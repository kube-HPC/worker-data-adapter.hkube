const zmq = require('zeromq');
const Flow = require('./Flow');
const WorkerQueue = require('./WorkerQueue');
const MessageQueue = require('./MessageQueue');
const { waitFor } = require('../../../../utils/waitFor');
const SIGNALS = require('./signals');

const HEARTBEAT_INTERVAL = 1000;

class ZMQProducer {
    constructor(port, maxMemorySize, responseAccumulator, consumerTypes, encoding, nodeName) {
        this._nodeName = nodeName;
        this._encoding = encoding;
        this._responseAccumulator = responseAccumulator;
        this._maxMemorySize = maxMemorySize;
        this._consumerTypes = consumerTypes;
        this._PPP_HEARTBEAT = Buffer.alloc(1, SIGNALS.PPP_HEARTBEAT);
        this._READY_SIGNALS = this._createSignals([
            SIGNALS.PPP_INIT,
            SIGNALS.PPP_READY,
            SIGNALS.PPP_HEARTBEAT,
            SIGNALS.PPP_DONE
        ]);
        this._DONE_SIGNALS = this._createSignals([
            SIGNALS.PPP_DONE,
            SIGNALS.PPP_DONE_DISCONNECT
        ]);
        this._NOT_READY_SIGNALS = this._createSignals([
            SIGNALS.PPP_NOT_READY,
            SIGNALS.PPP_DISCONNECT
        ]);
        this._active = true;
        this._waitingForResponse = new Map();
        this._messageQueue = new MessageQueue(consumerTypes, this._nodeName);
        this._workers = new WorkerQueue(this._consumerTypes);
        this._backend = zmq.socket('router'); // ROUTER
        this._backend.bind(`tcp://*:${port}`); // For workers
        console.log(`Producer listening on tcp://*:${port}`);
    }

    _createSignals(signals) {
        return signals.map(s => Buffer.alloc(1, s).toString('utf8'));
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
        this._backend.on('message', (...frames) => {
            const [workerAddress, workerSignal, consumer, result] = frames;
            const address = workerAddress.toString('utf8');
            const signal = workerSignal.toString('utf8');
            const consumerType = this._encoding.decode(consumer, { customEncode: false });

            if (!this._consumerTypes.includes(consumerType)) {
                console.log(`Producer got message from unknown consumer: ${consumerType}, dropping the message (consumers are ${this._consumerTypes.join(',')})`);
                return;
            }
            if (this._NOT_READY_SIGNALS.includes(signal)) {
                this._workers.notReady(consumerType, address);
                return;
            }
            if (this._DONE_SIGNALS.includes(signal)) {
                const sentTime = this._waitingForResponse.get(address);
                if (sentTime) {
                    this._waitingForResponse.delete(address);
                    const roundTripTime = (Date.now() - sentTime);
                    const { duration } = this._encoding.decode(result, { customEncode: false });
                    this._responseAccumulator(consumerType, duration, roundTripTime);
                }
            }
            if (!this._waitingForResponse.has(address) && this._READY_SIGNALS.includes(signal)) {
                this._workers.ready(consumerType, address);
            }

            this._doHeartbeat();
            this._handleMsg();
            this._workers.purge();
        });
    }

    _doHeartbeat() {
        if (Date.now() - this._lastHeartBeat < HEARTBEAT_INTERVAL) {
            return;
        }
        this._lastHeartBeat = Date.now();
        Object.entries(this._workers.queues).forEach(([, v]) => {
            v.forEach((worker) => {
                try {
                    this._backend.send([worker.address, this._PPP_HEARTBEAT]);
                }
                catch (e) {
                    if (this._active) {
                        console.log(e);
                    }
                }
            });
        });
    }

    _handleMsg() {
        Object.entries(this._workers.queues).forEach(([consumerType, queue]) => {
            if (queue) {
                const poped = this._messageQueue.pop(consumerType);
                if (poped) {
                    const { messageFlowPattern, header, payload } = poped;
                    const flow = new Flow(messageFlowPattern);
                    const address = this._workers.next(consumerType);
                    const flowMsg = this._encoding.encode(flow.getRestOfFlow(this._nodeName), { customEncode: false });
                    const frames = [
                        address,
                        SIGNALS.PPP_MSG,
                        flowMsg,
                        header,
                        payload
                    ];
                    try {
                        this._backend.send(frames);
                    }
                    catch (e) {
                        if (this._active) {
                            console.log(e);
                        }
                    }
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
