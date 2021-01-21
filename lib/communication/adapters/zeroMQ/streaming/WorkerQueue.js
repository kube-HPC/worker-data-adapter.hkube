class WorkerQueue {
    constructor(consumerTypes) {
        this.queues = {};
        consumerTypes.forEach((c) => {
            this.queues[c] = new Map();
        });
    }

    ready(worker, consumerType) {
        this.popItem(consumerType, worker.address);
        this.queues[consumerType].set(worker.address, worker);
    }

    purge() {
        const t = Date.now() / 1000;
        const expired = [];
        Object.entries(this.queues).forEach(([type, queue]) => {
            Object.entries(queue).forEach(([address, worker]) => {
                if (t > worker.expiry) {
                    expired.push({ address, type }); // Worker expired
                }
            });
        });
        expired.forEach((e) => {
            const { address, type } = e;
            console.log(`W: Idle worker expired: ${address}`);
            this.popItem(type, address);
        });
    }

    next(type) {
        const address = this.popItem(type);
        return address;
    }

    popItem(type, itemKey) {
        let key = itemKey;
        if (!key) {
            const entry = this.queues[type].entries().next().value;
            key = entry[0];
        }
        const item = this.queues[type].get(key);
        this.queues[type].delete(key);
        return item;
    }
}

module.exports = WorkerQueue;
