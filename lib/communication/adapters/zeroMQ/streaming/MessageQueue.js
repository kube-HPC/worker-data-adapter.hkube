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

    get sent() {
        return this._sent;
    }

    get lostMessages() {
        return this._lostMessages;
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
            const { messageFlowPattern } = this._queue[index];
            const flow = new Flow(messageFlowPattern);
            if (flow.isNextInFlow(consumerType, this._nodeName)) {
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
            let anyZero = false;
            this._indexPerConsumer.forEach((v) => {
                if (v === 0) {
                    anyZero = true;
                    return true;
                }
            });
            if (!anyZero) {
                this._queue.shift();
                const { payload } = out;
                this._sizeSum -= payload.length;
                this._indexPerConsumer.forEach((v, k) => {
                    this._indexPerConsumer.set(k, v - 1);
                });
            }
            return out;
        }
        return null;
    }

    loseMessage() {
        const out = this._queue.shift();
        const { msg } = out;
        this._sizeSum -= msg.length;
        this._indexPerConsumer.forEach(([k, v]) => {
            this._indexPerConsumer.set(k, v - 1);
            if (this._indexPerConsumer.get(k) > 0) {
                this._indexPerConsumer.set(k, v - 1);
            }
            else {
                this._lostMessages[k] += 1;
            }
        });
    }

    append({ messageFlowPattern, header, payload }) {
        this._sizeSum += payload.length;
        const flow = new Flow(messageFlowPattern);
        this._consumerTypes.forEach((c) => {
            if (flow.isNextInFlow(c, this._nodeName)) {
                this._everAppended[c] += 1;
            }
        });
        return this._queue.push({ messageFlowPattern, header, payload });
    }

    size(consumerType) {
        const everAppended = this._everAppended[consumerType];
        const size = everAppended - this._sent[consumerType];
        return size;
    }
}

module.exports = MessageQueue;
