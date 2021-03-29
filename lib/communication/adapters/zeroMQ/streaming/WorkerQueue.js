const Worker = require('./Worker');
class WorkerQueue {
    constructor(consumerTypes) {
        this.queues = {};
        consumerTypes.forEach((c) => {
            this.queues[c] = new Map();
        });
    }

    ready(consumerType, address) {
        const worker = new Worker(address);
        this._removeWorker(consumerType, address);
        this._addWorker(consumerType, worker);
    }

    notReady(consumerType, address) {
        this._removeWorker(consumerType, address);
    }

    purge() {
        const time = Date.now() / 1000;
        const expired = [];
        Object.entries(this.queues).forEach(([type, queue]) => {
            Object.entries(queue).forEach(([address, worker]) => {
                if (time > worker.expiry) {
                    expired.push({ address, type }); // Worker expired
                }
            });
        });
        expired.forEach((e) => {
            const { address, type } = e;
            console.log(`W: Idle worker expired: ${address}`);
            this._removeWorker(type, address);
        });
    }

    next(type) {
        // HANDLE NULL
        const entry = this.queues[type].entries().next().value;
        if (entry) {
            const key = entry[0];
            const item = this._getWorker(type, key);
            this._removeWorker(type, item.address);
            return item.address;
        }
        return null;
    }

    _getWorker(consumerType, address) {
        return this.queues[consumerType].get(address);
    }

    _addWorker(consumerType, worker) {
        // HANDLE ELSE
        if (this.queues[consumerType]) {
            this.queues[consumerType].set(worker.address, worker);
        }
    }

    _removeWorker(consumerType, address) {
        // HANDLE ELSE
        if (this.queues[consumerType]) {
            this.queues[consumerType].delete(address);
        }
    }
}

module.exports = WorkerQueue;
