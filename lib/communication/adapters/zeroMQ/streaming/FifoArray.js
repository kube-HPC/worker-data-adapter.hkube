class FifoArray {
    constructor(size) {
        this.arr = [];
        this.size = size;
    }

    append(obj) {
        this.arr.push(obj);
        if (this.arr.length > this.size) {
            this.arr.shift();
        }
    }

    getAsArray() {
        return this.arr;
    }

    reset() {
        this.arr = [];
    }
}

module.exports = FifoArray;
