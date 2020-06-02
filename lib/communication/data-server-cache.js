
class DataServerCache {
    constructor(config) {
        this._cache = new Map();
        this._maxCacheSize = config.maxCacheSize;
    }

    update(key, value) {
        if (!this.has(key) && this.size() >= this._maxCacheSize) {
            this._removeOldest();
        }
        this._cache.set(key, { timestamp: Date.now(), value });
    }

    _removeOldest() {
        let oldest = null;
        this._cache.forEach((value, key) => {
            if (!oldest) {
                oldest = key;
            }
            else if (value.timestamp < this._cache.get(oldest).timestamp) {
                oldest = key;
            }
        });
        this._cache.delete(oldest);
    }

    get(key) {
        const item = this._cache.get(key);
        return item.value;
    }

    has(key) {
        return this._cache.has(key);
    }

    size() {
        return this._cache.size;
    }
}

module.exports = DataServerCache;
