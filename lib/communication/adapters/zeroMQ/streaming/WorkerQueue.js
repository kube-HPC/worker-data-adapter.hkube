class WorkerQueue {
    constructor(consumerTypes) {
        this.queues = {};
        consumerTypes.forEach((c) => {
            this.queues[c] = new Map();
        });
    }

    ready(worker, consumerType) {
        this._removeWorker(consumerType, worker.address);
        this._addWorker(consumerType, worker);
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
        const entry = this.queues[type].entries().next().value;
        const key = entry[0];
        const item = this._getWorker(type, key);
        this._removeWorker(type, item.address);
        return item.address;
    }

    _getWorker(consumerType, address) {
        return this.queues[consumerType].get(address);
    }

    _addWorker(consumerType, worker) {
        this.queues[consumerType].set(worker.address, worker);
    }

    _removeWorker(consumerType, address) {
        this.queues[consumerType].delete(address);
    }
}

module.exports = WorkerQueue;
