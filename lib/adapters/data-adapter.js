const EventEmitter = require('events');
const objectPath = require('object-path');
const flatten = require('flat');
const clone = require('clone');
const isEqual = require('lodash.isequal');
const storageManager = require('@hkube/storage-manager');
const { DataRequest } = require('../communication/dataClient');
const Events = require('../consts/events');

class DataAdapter extends EventEmitter {
    async init(options) {
        this.enableCache = options.enableCache;
        this.storageCache = Object.create(null);
        this.oldStorage = null;
        await storageManager.init(options);
    }

    async getData(input, storage, tracerStart) {
        if (!storage || Object.keys(storage).length === 0) {
            return input;
        }
        const result = clone(input);
        const flatObj = flatten(input);
        if (this.enableCache && !this._isStorageEqual(storage, this.oldStorage)) {
            this.storageCache = Object.create(null);
        }
        this.oldStorage = storage;

        const promises = Object.entries(flatObj).map(async ([k, v]) => {
            if (typeof v === 'string' && v.startsWith('$$')) {
                const key = v.substring(2);
                const link = storage[key];
                if (!link) {
                    throw new Error('unable to find storage key');
                }
                let data = null;
                if (Array.isArray(link)) {
                    data = await Promise.all(link.map(a => a && this._tryGetDataFromPeerOrStorage(a, tracerStart)));
                }
                else {
                    data = await this._tryGetDataFromPeerOrStorage(link, tracerStart);
                }
                objectPath.set(result, k, data);
            }
        });
        await Promise.all(promises);
        return result;
    }

    async setData(options, tracer) {
        const { jobId, taskId, data } = options;
        const newData = data === undefined ? null : data;
        const result = await storageManager.hkube.put({ jobId, taskId, data: newData }, tracer);
        return result;
    }

    createStorageInfo(options) {
        const { jobId, taskId, nodeName, data, savePaths } = options;
        const path = storageManager.hkube.createPath({ jobId, taskId });
        const metadata = this.createMetadata({ nodeName, data, savePaths });
        const storageInfo = { storageInfo: { path }, metadata };
        return storageInfo;
    }

    createStoragePath(options) {
        const { jobId, taskId } = options;
        return storageManager.hkube.createPath({ jobId, taskId });
    }

    createMetadata({ nodeName, data, savePaths }) {
        const object = { [nodeName]: data };
        const paths = savePaths || [];
        const metadata = Object.create(null);
        paths.forEach((p) => {
            const value = objectPath.get(object, p, 'DEFAULT');
            if (value !== 'DEFAULT') {
                this._setMetadata(value, p, metadata);
            }
        });
        return metadata;
    }

    _setMetadata(value, path, metadata) {
        metadata[path] = Array.isArray(value) // eslint-disable-line
            ? { type: 'array', size: value.length }
            : { type: typeof (value) };
    }

    async _tryGetDataFromPeerOrStorage(options, trace) {
        let data;
        const { path, index } = options;
        const dataPath = this._createDataPath(path, index);

        if (options.discovery) {
            data = await this._getFromPeer({ ...options, dataPath });
        }
        if (data === undefined && options.storageInfo) {
            data = await this._getFromStorage(options, trace);
            if (dataPath) {
                data = objectPath.get(data, dataPath);
            }
        }
        return data;
    }

    _createDataPath(path, index) {
        let dataPath = path;
        if (Number.isInteger(index)) {
            dataPath = (path && `${path}.${index}`) || `${index}`;
        }
        return dataPath;
    }

    async _getFromPeer(options) {
        const { taskId, dataPath } = options;
        const { port, host, encoding } = options.discovery;
        const dataRequest = new DataRequest({ address: { port, host }, taskId, dataPath, encoding });
        const response = await dataRequest.invoke();
        this.emit(Events.DiscoveryGet, response);
        return response.data;
    }

    async _getFromStorage(options, trace) {
        let data;
        if (this.enableCache) {
            data = this.storageCache[options.storageInfo.path];
        }
        if (data === undefined) {
            data = await storageManager.get(options.storageInfo, trace);
            this.emit(Events.StorageGet, data);
        }
        if (this.enableCache) {
            this.storageCache[options.storageInfo.path] = data;
        }
        return data;
    }

    _isStorageEqual(storage1, storage2) {
        if (storage1 && storage2) {
            const links1 = Object.values(storage1).map(s => s.storageInfo.path).sort();
            const links2 = Object.values(storage2).map(s => s.storageInfo.path).sort();
            return isEqual(links1, links2);
        }

        return storage1 === storage2;
    }
}

module.exports = new DataAdapter();
