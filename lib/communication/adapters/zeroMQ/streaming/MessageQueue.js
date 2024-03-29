const Flow = require('./Flow');

class MessageQueue {
    constructor(consumerTypes, nodeName) {
        this._nodeName = nodeName;
        this._consumerTypes = consumerTypes;
        this._indexPerConsumer = new Map();
        this._sent = {};
        this._everAppended = {};
        this._lostMessages = {};
        this._consumerTypes.forEach((c) => {
            this._indexPerConsumer.set(c, 0);
            this._sent[c] = 0;
            this._everAppended[c] = 0;
            this._lostMessages[c] = 0;
        });
        this._sizeSum = 0;
        this._queue = [];
    }

    get sizeSum() {
        return this._sizeSum;
    }

    get maxMemorySize() {
        return this._maxMemorySize;
    }

    get sent() {
        return this._sent;
    }

    lostMessages(cunsumerType) {
        return this._lostMessages[cunsumerType];
    }

    get queue() {
        return this._queue;
    }

    hasItems(consumerType) {
        return this._indexPerConsumer.get(consumerType) < this._queue.length;
    }

    _nextMessageIndex(consumerType) {
        let index = this._indexPerConsumer.get(consumerType);
        let foundMessage = false;
        while (!foundMessage && index < this._queue.length) {
            const { flow } = this._queue[index];
            const streamFlow = new Flow(flow);
            if (streamFlow.isNextInFlow(consumerType, this._nodeName)) {
                foundMessage = true;
            }
            else {
                index += 1;
            }
        }
        if (foundMessage) {
            return index;
        }
        return null;
    }

    // Messages are kept in the queue until consumers of all types popped out the message.
    // An index per consumer type is maintained, to know which messages the consumer already received and conclude which message should he get now.
    pop(consumerType) {
        const nextItemIndex = this._nextMessageIndex(consumerType);
        if (nextItemIndex !== null) {
            const out = this._queue[nextItemIndex];
            this._indexPerConsumer.set(consumerType, nextItemIndex + 1);
            this._sent[consumerType] += 1;
            while (this._removeIfNeeded()) {
                continue; // eslint-disable-line
            }
            return out;
        }
        return null;
    }

    _removeIfNeeded() {
        if (this._queue.length === 0) {
            return false;
        }
        let anyZero = false;
        const out = this._queue[0];
        this._indexPerConsumer.forEach((v, k) => {
            if (v === 0) {
                const { flow } = out;
                const streamFlow = new Flow(flow);
                if (streamFlow.isNextInFlow(k, this._nodeName)) {
                    anyZero = true;
                }
            }
        });
        if (!anyZero) {
            this._queue.shift();
            const { payload } = out;
            this._sizeSum -= payload.length;
            this._indexPerConsumer.forEach((v, k) => {
                if (this._indexPerConsumer.get(k) > 0) {
                    this._indexPerConsumer.set(k, v - 1);
                }
            });
            return true;
        }
        return false;
    }

    loseMessage() {
        const out = this._queue.shift();
        if (!out) {
            return;
        }
        const { payload, flow } = out;
        this._sizeSum -= payload.length;
        this._indexPerConsumer.forEach((v, k) => {
            if (this._indexPerConsumer.get(k) > 0) {
                this._indexPerConsumer.set(k, v - 1);
            }
            else {
                const streamFlow = new Flow(flow);
                if (streamFlow.isNextInFlow(k, this._nodeName)) {
                    this._lostMessages[k] += 1;
                }
            }
        });
    }

    append({ header, payload, flow }) {
        let hasRecipient = false;
        const streamFlow = new Flow(flow);
        this._consumerTypes.forEach((c) => {
            if (streamFlow.isNextInFlow(c, this._nodeName)) {
                this._everAppended[c] += 1;
                hasRecipient = true;
            }
        });
        if (hasRecipient) {
            this._sizeSum += payload.length;
            this._queue.push({ flow, header, payload });
        }
    }

    size(consumerType) {
        const everAppended = this._everAppended[consumerType];
        const lost = this._lostMessages[consumerType];
        const sent = this._sent[consumerType];
        const size = everAppended - lost - sent;
        return size;
    }
}

module.exports = MessageQueue;
