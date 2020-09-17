const EventEmitter = require('events');
const objectPath = require('object-path');
const flatten = require('flat');
const queue = require('async/queue');
const { Encoding } = require('@hkube/encoding');
const storageManager = require('@hkube/storage-manager');
const { DataRequest } = require('../communication/data-client');
const Cache = require('../cache/cache');

class DataAdapter extends EventEmitter {
    async init(options, dataServer) {
        this._dataServer = dataServer;
        this.storageCache = Object.create(null);
        await storageManager.init(options);
        const storage = options.storageAdapters[options.defaultStorage];
        this._cache = new Cache(storage);
        this._encoding = new Encoding({ type: storage.encoding });
        this._requestEncoding = options.discovery.encoding;
        this._requestTimeout = options.discovery.timeout;
        this._networkTimeout = options.discovery.networkTimeout;
        this._queue = queue(this._getFromPeerInternal.bind(this), options.discovery.concurrency);
    }

    encode(value, encodeOptions) {
        return this._encoding.encode(value, encodeOptions);
    }

    decode(value, encodeOptions) {
        return this._encoding.decode(value, encodeOptions);
    }

    async getData({ jobId, input, flatInput, storage, tracerStart }) {
        if (!flatInput || Object.keys(flatInput).length === 0) {
            return input;
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
                    data = await this._batchRequest(link, jobId);
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
        const { data } = options;
        const newData = data === undefined ? null : data;
        const path = this.createStoragePath(options);
        const result = await storageManager.storage.put({ path, data: newData, encodeOptions: { ignoreEncode: true } }, tracer);
        return result;
    }

    createStorageInfo(options) {
        const { jobId, taskId, nodeName, data, encodedData, savePaths } = options;
        const path = this.createStoragePath({ jobId, taskId });
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

    async _batchRequest(options, jobId) {
        const batchResponse = [];

        await Promise.all(options.map(async d => {
            const { storageInfo, path: dataPath } = d;
            if (storageInfo) {
                const storageResult = await this._getFromCacheOrStorage(storageInfo, storageInfo.path, dataPath);
                batchResponse.push(storageResult);
                return;
            }
            const { valuesInCache, valuesNotInCache } = this._cache.getAll(d.tasks);
            if (valuesNotInCache.length > 0) {
                const results = await this._getFromPeer({ ...d, tasks: valuesNotInCache });
                await Promise.all(results.map(async (r, i) => {
                    const { size, content } = r;
                    const peerError = this._getPeerError(content);
                    const taskId = valuesNotInCache[i];
                    if (peerError) {
                        const storageData = await this._getDataForTask(jobId, taskId, dataPath);
                        batchResponse.push(storageData);
                    }
                    else {
                        this._cache.update(taskId, content, size);
                        const result = this._getPath(content, dataPath);
                        batchResponse.push(result);
                    }
                }));
            }
            batchResponse.push(...valuesInCache);
        }));
        return batchResponse;
    }

    _getDataForTask(jobId, taskId, dataPath) {
        const path = this.createStoragePath({ jobId, taskId });
        return this._getFromCacheOrStorage({ path }, taskId, dataPath);
    }

    async _tryGetDataFromPeerOrStorage(options, trace) {
        const { path: dataPath, taskId, discovery, storageInfo } = options;
        let hasResponse = false;
        let cacheId;
        if (discovery) {
            cacheId = taskId;
        }
        else {
            cacheId = storageInfo.path;
        }
        let data = this._getFromCache(cacheId, dataPath);

        if (data === undefined) {
            if (discovery) {
                const peerResult = await this._getFromPeer({ ...options, dataPath });
                const { size, content } = peerResult[0];
                const peerError = this._getPeerError(content);
                hasResponse = peerError === undefined;
                data = hasResponse ? content : undefined;
                if (hasResponse) {
                    this._setToCache(cacheId, content, size);
                    data = this._getPath(data, dataPath);
                }
            }
            if (!hasResponse && storageInfo) {
                data = await this._getValueFromStorage(storageInfo, cacheId, dataPath, trace);
            }
        }
        return data;
    }

    async _getFromPeer(options) {
        return this._queue.pushAsync(options);
    }

    async _getFromPeerInternal(options) {
        const { taskId } = options;
        const { port, host } = options.discovery;
        const tasks = taskId ? [taskId] : options.tasks;

        let responses;
        if (this._dataServer && host === this._dataServer._host && port === this._dataServer._port) {
            const dataList = this._dataServer.getDataByTasks(tasks);
            responses = dataList.map(d => ({ size: d.value.length, content: this.decode(d.value) }));
        }
        else {
            const dataRequest = new DataRequest({
                address: { port, host },
                tasks,
                encoding: this._requestEncoding,
                timeout: this._requestTimeout,
                networkTimeout: this._networkTimeout
            });
            responses = await dataRequest.invoke();
        }
        return responses;
    }

    _getPeerError(options) {
        let error;
        if (options && options.hkube_error) {
            error = options.hkube_error;
        }
        return error;
    }

    async _getFromCacheOrStorage(options, cacheId, dataPath, trace) {
        let data = this._getFromCache(cacheId);
        if (data === undefined) {
            data = await this._getValueFromStorage(options, cacheId, dataPath, trace);
        }
        return data;
    }

    async _getValueFromStorage(options, cacheId, dataPath, trace) {
        const { size, response } = await this._getFromStorage(options, trace);
        this._setToCache(cacheId, response, size);
        const data = this._getPath(response, dataPath);
        return data;
    }

    _getFromCache(cacheId, dataPath) {
        const data = this._cache.get(cacheId);
        const result = this._getPath(data, dataPath);
        return result;
    }

    _setToCache(cacheId, data, size) {
        this._cache.update(cacheId, data, size);
    }

    _getPath(data, dataPath) {
        let newData;
        if (data && dataPath) {
            newData = objectPath.get(data, dataPath);
        }
        else {
            newData = data;
        }
        return newData;
    }

    async _getFromStorage(options, trace) {
        const data = await storageManager.get({ ...options, encodeOptions: { ignoreEncode: true } }, trace);
        const response = this.decode(data);
        const size = data.length;
        return { size, response };
    }
}

module.exports = new DataAdapter();
