const HEARTBEAT_LIVENESS = 5; // 3..5 is reasonable
const HEARTBEAT_INTERVAL = 1; // Seconds

class Worker {
    constructor(address) {
        this.address = address;
        this.expiry = (Date.now() / 1000) + (HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS);
    }
}

module.exports = Worker;
