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
        this.storageCache = Object.create(null);
        this.oldStorage = null;
        await storageManager.init(options);
    }

    async getData(input, storage, tracerStart) {
        if (!input || input.length === 0) {
            return input;
        }
        if (!storage || Object.keys(storage).length === 0) {
            return input;
        }
        const result = clone(input);
        const flatObj = flatten(input);
        if (!this._isStorageEqual(storage, this.oldStorage)) {
            this.storageCache = Object.create(null);
        }
        this.oldStorage = storage;

        const promises = Object.entries(flatObj).map(async ([k, v]) => {
            if (this._isStorage(v)) {
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

    _isStorage(value) {
        return typeof value === 'string' && value.startsWith('$$');
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
                const meta = this._getMetadata(value);
                metadata[p] = meta;
            }
        });
        return metadata;
    }

    _getMetadata(value) {
        const meta = Array.isArray(value)
            ? { type: 'array', size: value.length }
            : { type: typeof (value) };
        return meta;
    }

    async _tryGetDataFromPeerOrStorage(options, trace) {
        let data;
        const { path, discovery, storageInfo } = options;

        if (discovery) {
            data = await this._getFromPeer({ ...options, dataPath: path });
        }
        if (data === undefined && storageInfo) {
            data = await this._getFromStorage(storageInfo, trace);
            if (path) {
                data = objectPath.get(data, path);
            }
        }
        return data;
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
        let data = this.storageCache[options.path];

        if (data === undefined) {
            data = await storageManager.get(options, trace);
            this.emit(Events.StorageGet, data);
        }

        this.storageCache[options.path] = data;

        return data;
    }

    _isStorageEqual(storage1, storage2) {
        if (storage1 && storage2) {
            const flatObj1 = flatten(storage1);
            const flatObj2 = flatten(storage2);

            const links1 = Object.entries(flatObj1).filter(([k, v]) => k.endsWith('storageInfo.path')).map(v => v[1]).sort();
            const links2 = Object.entries(flatObj2).filter(([k, v]) => k.endsWith('storageInfo.path')).map(v => v[1]).sort();

            return isEqual(links1, links2);
        }

        return storage1 === storage2;
    }
}

module.exports = new DataAdapter();
