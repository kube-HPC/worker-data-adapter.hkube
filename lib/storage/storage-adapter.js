const objectPath = require('object-path');
const flatten = require('flat');
const clone = require('clone');
const isEqual = require('lodash.isequal');
const storageManager = require('@hkube/storage-manager');
const DataRequest = require('../communication/dataClient');

class StorageAdapter {
    async init(options) {
        this.enableCache = options.enableCache;
        this.storageCache = Object.create(null);
        this.oldStorage = null;
        await storageManager.init(options);
    }

    async getData(input, storage, tracerStart) {
        const result = clone(input);
        const flatObj = flatten(input);
        if (this.enableCache && !this._isStorageEqual(storage, this.oldStorage)) {
            this.storageCache = Object.create(null);
        }
        this.oldStorage = storage;

        const promiseDataExtractors = Object.entries(flatObj).map(async ([k, v]) => {
            if (typeof v === 'string' && v.startsWith('$$')) {
                const key = v.substring(2);
                const link = storage[key];
                if (!link) {
                    throw new Error('unable to find storage key');
                }
                let data = null;
                if (Array.isArray(link)) {
                    data = await Promise.all(link.map(a => a && this._tryGetDataFromPeerAndStorage(a, tracerStart)));
                }
                else {
                    data = await this._tryGetDataFromPeerAndStorage(link, tracerStart);
                }
                objectPath.set(result, k, data);
            }
        });
        await Promise.all(promiseDataExtractors);
        return result;
    }

    async setData(options) {
        const { jobId, taskId, data } = options;
        const newData = data === undefined ? null : data;
        const result = await storageManager.hkube.put({ jobId, taskId, data: newData });
        return result;
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
        metadata[path] = Array.isArray(value)
            ? { type: 'array', size: value.length }
            : { type: typeof (value) };
    }

    async _tryGetDataFromPeerAndStorage(options, trace) {
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
        const { port, host } = options.discovery;
        const dataRequest = new DataRequest({ port, host, taskId, dataPath, binary: true });
        const response = await dataRequest.invoke();
        return response.data;
    }

    async _getFromStorage(options, trace) {
        let data;
        if (this.enableCache) {
            data = this.storageCache[options.storageInfo.path];
        }
        if (data === undefined) {
            data = await storageManager.get(options.storageInfo, trace);
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

module.exports = new StorageAdapter();
