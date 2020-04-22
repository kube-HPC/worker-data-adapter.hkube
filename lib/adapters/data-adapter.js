const EventEmitter = require('events');
const objectPath = require('object-path');
const flatten = require('flat');
const { Encoding } = require('@hkube/encoding');
const storageManager = require('@hkube/storage-manager');
const { DataRequest } = require('../communication/dataClient');

class DataAdapter extends EventEmitter {
    async init(options, dataServer) {
        this._dataServer = dataServer;
        this.storageCache = Object.create(null);
        await storageManager.init(options);
        const storage = options.storageAdapters[options.defaultStorage];
        this._encoding = new Encoding({ type: storage.encoding });
        console.log(`init ${options.defaultStorage} storage client with ${storage.encoding} encoding`);
    }

    encode(value) {
        return this._encoding.encode(value);
    }

    decode(value) {
        return this._encoding.decode(value);
    }

    async getData({ input, flatInput, useCache, storage, tracerStart }) {
        if (!flatInput || Object.keys(flatInput).length === 0) {
            return input;
        }
        if (!useCache) {
            this.storageCache = Object.create(null);
        }
        const promises = Object.entries(flatInput).map(async ([k, v]) => {
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
                objectPath.set(input, k, data);
            }
        });
        await Promise.all(promises);
        return input;
    }

    _isStorage(value) {
        return typeof value === 'string' && value.startsWith('$$');
    }

    flatInput({ input, storage }) {
        if (!input || input.length === 0) {
            return {};
        }
        if (!storage || Object.keys(storage).length === 0) {
            return {};
        }
        const flatInput = flatten(input);
        return flatInput;
    }

    async setData(options, tracer) {
        const { jobId, taskId, data } = options;
        const newData = data === undefined ? null : data;
        const result = await storageManager.hkube.put({ jobId, taskId, data: newData }, tracer);
        return result;
    }

    createStorageInfo(options) {
        const { jobId, taskId, nodeName, data, encodedData, savePaths } = options;
        const path = storageManager.hkube.createPath({ jobId, taskId });
        const metadata = this.createMetadata({ nodeName, data, savePaths });
        const storageInfo = { storageInfo: { path, size: encodedData && encodedData.length }, metadata };
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
        if (data == null && storageInfo) {
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

        let response;
        if (this._dataServer && host === this._dataServer._host && port === this._dataServer._port) {
            response = this._dataServer.createData({ taskId, dataPath });
        }
        else {
            const dataRequest = new DataRequest({ address: { port, host }, taskId, dataPath, encoding });
            response = await dataRequest.invoke();
        }

        if (Object.prototype.toString.call(response) === '[object Object]' && response.error) {
            console.error(JSON.stringify(response.error));
            return undefined;
        }
        return response;
    }

    async _getFromStorage(options, trace) {
        let data = this.storageCache[options.path];
        if (data === undefined) {
            data = await storageManager.get(options, trace);
            this.storageCache[options.path] = data;
        }
        return data;
    }
}

module.exports = new DataAdapter();
