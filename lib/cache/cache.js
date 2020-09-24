const MB = 1024 * 1024;

class Cache {
    constructor(config) {
        this._cache = new Map();
        this._maxCacheSize = config.maxCacheSize * MB;
        this._size = 0;
    }

    update(key, value, size, header = null) {
        if (this.has(key)) {
            return true;
        }
        if (size > this._maxCacheSize) {
            console.warn(`unable to insert cache value of size ${size / MB} MB, max: ${this._maxCacheSize / MB} MB`);
            return false;
        }
        while ((this._size + size) > this._maxCacheSize) {
            this._removeOldest();
        }
        this._cache.set(key, { timestamp: Date.now(), header, value, size });
        this._size += size;
        return true;
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
        this._size -= this._cache.get(oldest).size;
        this._cache.delete(oldest);
    }

    get(key) {
        const item = this._cache.get(key);
        return item && item.value;
    }

    getWithHeader(key) {
        const item = this._cache.get(key);
        return item && { header: item.header, value: item.value };
    }

    getAll(keys) {
        const valuesInCache = [];
        const valuesNotInCache = [];
        keys.forEach(k => {
            const cacheRecord = this._cache.get(k);
            if (cacheRecord) {
                valuesInCache.push(cacheRecord.value);
            }
            else {
                valuesNotInCache.push(k);
            }
        });
        return { valuesInCache, valuesNotInCache };
    }

    has(key) {
        return this._cache.has(key);
    }

    get size() {
        return this._size;
    }

    get count() {
        return this._cache.size;
    }
}

module.exports = Cache;
