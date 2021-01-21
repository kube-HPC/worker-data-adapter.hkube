const cloneDeep = require('lodash.clonedeep');

class Flow {
    constructor(flow) {
        this._flow = flow;
        this._flowCopy = cloneDeep(flow);
    }

    isNextInFlow(next, currentName) {
        const { item } = this._getCurrent(currentName);
        if (!item) {
            return false;
        }
        return item.next.includes(next);
    }

    getRestOfFlow(currentName) {
        const { index, item } = this._getCurrent(currentName);
        if (!item) {
            return [];
        }
        this._flowCopy.splice(index, 1);
        return this._flowCopy;
    }

    _getCurrent(currentName) {
        const index = this._flow.findIndex(n => n.source === currentName);
        return {
            index,
            item: this._flow[index]
        };
    }
}

module.exports = Flow;
