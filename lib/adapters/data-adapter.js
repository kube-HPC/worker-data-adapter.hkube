const EventEmitter = require('events');
const objectPath = require('object-path');
const flatten = require('flat');
const { Encoding } = require('@hkube/encoding');
const storageManager = require('@hkube/storage-manager');
const { DataRequest } = require('../communication/data-client');

class DataAdapter extends EventEmitter {
    async init(options, dataServer) {
        this._dataServer = dataServer;
        this.storageCache = Object.create(null);
        await storageManager.init(options);
        const storage = options.storageAdapters[options.defaultStorage];
        this._encoding = new Encoding({ type: storage.encoding });
        this._requestEncoding = options.discovery.encoding;
        this._requestTimeout = options.discovery.timeout;
    }

    encode(value, encodeOptions) {
        return this._encoding.encode(value, encodeOptions);
    }

    decode(value, encodeOptions) {
        return this._encoding.decode(value, encodeOptions);
    }

    async getData({ jobId, input, flatInput, useCache, storage, tracerStart }) {
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
            const { storageInfo, tasks, path: dataPath } = d;
            if (storageInfo) {
                const storageResult = await this._getFromCacheOrStorage(storageInfo, dataPath);
                batchResponse.push(storageResult);
                return;
            }

            const peerResponse = await this._getFromPeer({ ...d, dataPath });
            const peerError = this._getPeerError(peerResponse);

            if (peerError) {
                console.log(`batch request has failed with ${peerError.message}, using storage fallback`);

                const storageData = await Promise.all(tasks.map(t => {
                    const path = storageManager.hkube.createPath({ jobId, taskId: t });
                    return this._getFromCacheOrStorage({ path }, dataPath);
                }));
                batchResponse.push(...storageData);
            }
            else {
                const { errors, items } = peerResponse;

                if (errors) {
                    const results = await Promise.all(items.map((t, i) => {
                        const peerErr = this._getPeerError(t);
                        if (peerErr) {
                            const taskId = tasks[i];
                            const path = storageManager.hkube.createPath({ jobId, taskId });
                            return this._getFromCacheOrStorage({ path }, dataPath);
                        }
                        return t;
                    }));
                    batchResponse.push(...results);
                }
                else {
                    batchResponse.push(...items);
                }
            }
        }));
        return batchResponse;
    }

    async _tryGetDataFromPeerOrStorage(options, trace) {
        let data;
        const { path: dataPath, discovery, storageInfo } = options;
        let hasResponse = false;

        if (discovery) {
            data = await this._getFromPeer({ ...options, dataPath });
            const peerError = this._getPeerError(data);
            hasResponse = peerError === undefined;
            data = peerError ? undefined : data;
        }
        if (!hasResponse && storageInfo) {
            data = await this._getFromCacheOrStorage(storageInfo, dataPath, trace);
        }
        return data;
    }

    async _getFromPeer(options) {
        const { tasks, taskId, dataPath } = options;
        const { port, host } = options.discovery;

        let response;
        if (this._dataServer && host === this._dataServer._host && port === this._dataServer._port) {
            response = this._dataServer.createData({ tasks, taskId, dataPath });
        }
        else {
            const dataRequest = new DataRequest({
                address: { port, host },
                tasks,
                taskId,
                dataPath,
                encoding: this._requestEncoding,
                timeout: this._requestTimeout
            });
            response = await dataRequest.invoke();
        }
        return response;
    }

    _getPeerError(options) {
        let error;
        if (options && options.hkube_error) {
            error = options.hkube_error;
        }
        return error;
    }

    async _getFromCacheOrStorage(options, dataPath, trace) {
        const { path } = options;
        let data = this._getFromCache(path);
        if (data === undefined) {
            data = await this._getFromStorage(options, trace);
            this.storageCache[options.path] = data;
        }
        if (dataPath) {
            data = objectPath.get(data, dataPath);
        }
        return data;
    }

    _getFromCache(path) {
        return this.storageCache[path];
    }

    async _getFromStorage(options, trace) {
        const response = await storageManager.get({ ...options, encodeOptions: { customEncode: true } }, trace);
        return response;
    }
}

module.exports = new DataAdapter();
