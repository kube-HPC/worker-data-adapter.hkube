const zmq = require('zeromq');
const Flow = require('./Flow');
const Worker = require('./Worker');
const WorkerQueue = require('./WorkerQueue');
const MessageQueue = require('./MessageQueue');
const { waitFor } = require('../../../../utils/waitFor');

const PPP_READY = 0x01; // Signals worker is ready
const PPP_HEARTBEAT = 0x02; // Signals worker heartbeat
const HEARTBEAT_INTERVAL = 1000;

class ZMQProducer {
    constructor(port, maxMemorySize, responseAccumulator, consumerTypes, encoding, nodeName) {
        this._nodeName = nodeName;
        this._encoding = encoding;
        this._responseAccumulator = responseAccumulator;
        this._maxMemorySize = maxMemorySize;
        this._consumerTypes = consumerTypes;
        this._PPP_READY = Buffer.alloc(1, PPP_READY);
        this._PPP_HEARTBEAT = Buffer.alloc(1, PPP_HEARTBEAT);
        this._KEEP_ALIVE = [this._PPP_READY.toString('utf8'), this._PPP_HEARTBEAT.toString('utf8')];
        this._messageQueue = new MessageQueue(consumerTypes, this._nodeName);
        this._workers = new WorkerQueue(this._consumerTypes);
        this._backend = zmq.socket('router'); // ROUTER
        this._backend.setsockopt(zmq.ZMQ_LINGER, 0);
        this._backend.bind(`tcp://*:${port}`); // For workers
        console.log(`Producer listening on tcp://*:${port}`);
        this._active = true;
        this._heartbeatInterval = setInterval(this._doHeartbeat.bind(this), HEARTBEAT_INTERVAL);
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
            const address = this._encoding.decode(frames[0], { customEncode: false });
            const workerReady = frames[1];
            const consumerType = this._encoding.decode(frames[2], { customEncode: false });

            if (!this._KEEP_ALIVE.includes(workerReady.toString('utf8'))) {
                this._responseAccumulator(workerReady, consumerType);
            }
            this._workers.ready(new Worker(address), consumerType);
            this._handleMsg(this._workers);
            this._workers.purge();
        });
    }

    _doHeartbeat() {
        Object.entries(this._workers.queues).forEach(([k, v]) => {
            v.forEach((worker) => {
                const address = this._encoding.encode(worker.address, { customEncode: false });
                try {
                    this._backend.send([address, this._PPP_HEARTBEAT]);
                }
                catch (e) {
                    if (this._active) {
                        console.log(e);
                    }
                    else {
                        return null;
                    }
                }
            });
        });
    }

    _handleMsg(workers) {
        Object.entries(workers.queues).forEach(([consumerType, queue]) => {
            if (queue) {
                const poped = this._messageQueue.pop(consumerType);
                if (poped) {
                    const { messageFlowPattern, header, payload } = poped;
                    const flow = new Flow(messageFlowPattern);
                    const workerAddress = workers.next(consumerType);
                    const address = this._encoding.encode(workerAddress, { customEncode: false });
                    const frames = [
                        address,
                        this._encoding.encode(flow.getRestOfFlow(this._nodeName), { customEncode: false }),
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
                        else {
                            return null;
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
        clearInterval(this._heartbeatInterval);
        this._heartbeatInterval = null;
        this._active = false;
        this._backend.close();
    }
}

module.exports = ZMQProducer;
