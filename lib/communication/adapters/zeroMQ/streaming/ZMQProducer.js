const zmq = require('zeromq');
const Flow = require('./Flow');
const Worker = require('./Worker');
const WorkerQueue = require('./WorkerQueue');
const MessageQueue = require('./MessageQueue');

const PPP_READY = 0x01; // Signals worker is ready
const PPP_HEARTBEAT = 0x02; // Signals worker heartbeat

class ZMQProducer {
    constructor(port, maxMemorySize, responseAccumulator, consumerTypes, encoding, nodeName) {
        this._nodeName = nodeName;
        this._encoding = encoding;
        this._responseAccumulator = responseAccumulator;
        this._maxMemorySize = maxMemorySize;
        this._port = port;
        this._consumerTypes = consumerTypes;
        this._messageQueue = new MessageQueue(consumerTypes, this._nodeName);
        this._backend = zmq.socket('router'); // ROUTER
        this._backend.bind(`tcp://*:${port}`); // For workers
        console.log(`Producer listening on tcp://*:${port}`);
        this._active = true;
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
        const workers = new WorkerQueue(this._consumerTypes);
        this._backend.on('message', (...args) => {
            const address = this._encoding.decode(args[0], { customEncode: false });
            const workerReady = args[1][0];
            const consumerType = this._encoding.decode(args[2], { customEncode: false });

            if (![PPP_HEARTBEAT, PPP_READY].includes(workerReady)) {
                this._responseAccumulator(workerReady, consumerType);
            }
            workers.ready(new Worker(address), consumerType);
            this._handleMsg(workers);
        });
    }

    _handleMsg(workers) {
        Object.entries(workers.queues).forEach(([consumerType, queue]) => {
            if (queue) {
                const poped = this._messageQueue.pop(consumerType);
                if (poped) {
                    const { messageFlowPattern, header, payload } = poped;
                    const flow = new Flow(messageFlowPattern);
                    const frames = [
                        workers.next(consumerType),
                        this._encoding.encode(flow.getRestOfFlow(this._nodeName), { customEncode: false }),
                        header,
                        payload
                    ];
                    try {
                        this._backend.send(frames);
                    }
                    catch (e) {
                        if (this._active)
                            console.log(e);
                        else
                            return;
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

    close(force = true) {

        setInterval(() => {
            if (this._messageQueue.queue && !force) {

            }
        }, 1000)

        this._active = false;
        this._backend.close();
    }
}

module.exports = ZMQProducer;
