class OrderedDict {
    constructor() {
        this.dict = {};
        this.arr = [];
    }

    set(key, val) {
        if (!(key in this.dict)) {
            this.arr.push(key);
        }
        this.dict[key] = val;
        return this.dict[key];

    }

    get(key) {
        return this.dict[key];

    }

    has(key) {
        return !!this.dict[key];
    }

    remove(key) {
        for (let i = 0, l = this.arr.length; i < l; i++) {

            if (this.arr[i] === key) {
                this.arr.splice(i, 1);
                delete this.dict[key];
            }
        }
    }

    insert(index, key, val) {

        if (this.arr.indexOf(key) !== -1) throw 'key `' + key + '` already exists';

        this.arr.splice(index, 0, key);
        this.dict[key] = val;

    }

    forEach(callback) {

        for (let i = 0, l = this.arr.length; i < l; i++) {

            let key = this.arr[i],
                val = this.dict[key];

            callback(val, key);
        }
    }

    size() {
        return this.arr.length;
    }

    keys() {
        return this.arr;
    }

    values() {
        let vals = [];
        let len = this.arr.length;

        for (let i = 0; i < len; i++) {
            vals.push(this.dict[this.arr[i]]);
        }

        return vals;
    }

    clear() {

        this.dict = {};
        this.arr = [];

    }

}

module.exports = OrderedDict;