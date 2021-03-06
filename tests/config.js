const config = module.exports = {};
config.clusterName = process.env.CLUSTER_NAME || 'local';
config.defaultStorage = process.env.DEFAULT_STORAGE || 'fs';
config.storageMode = 'v2';
config.enableCache = !!process.env.ENABLE_WORKER_CACHE;

config.socket = {
    port: process.env.WORKER_SOCKET_PORT || 3000,
    host: process.env.WORKER_SOCKET_HOST || 'localhost',
    protocol: process.env.WORKER_SOCKET_PROTOCOL || 'ws',
    url: process.env.WORKER_SOCKET_URL || null,
    encoding: process.env.WORKER_ALGORITHM_ENCODING || 'bson'
};

config.algorithm = {
    path: process.env.ALGORITHM_PATH || 'algorithm_unique_folder',
    entryPoint: process.env.ALGORITHM_ENTRY_POINT || 'index.js'
};

config.discovery = {
    host: process.env.POD_NAME || '127.0.0.1',
    port: process.env.DISCOVERY_PORT || 9020,
    encoding: process.env.DISCOVERY_ENCODING || 'msgpack',
    timeout: process.env.DISCOVERY_TIMEOUT || 1000,
    networkTimeout: process.env.DISCOVERY_NETWORK_TIMEOUT || 1000,
    maxCacheSize: process.env.DISCOVERY_MAX_CACHE_SIZE || 500,
    servingReportInterval: process.env.DISCOVERY_SERVING_REPORT_INTERVAL || 5000,
};

config.s3 = {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    endpoint: process.env.S3_ENDPOINT_URL || 'http://127.0.0.1:9000'
};

config.fs = {
    baseDirectory: process.env.BASE_FS_ADAPTER_DIRECTORY || '/var/tmp/fs/storage'
};

config.storageAdapters = {
    s3: {
        connection: config.s3,
        encoding: process.env.STORAGE_ENCODING || 'msgpack',
        maxCacheSize: process.env.STORAGE_MAX_CACHE_SIZE_MB || 500,
        moduleName: process.env.STORAGE_MODULE || '@hkube/s3-adapter'
    },
    fs: {
        connection: config.fs,
        encoding: process.env.STORAGE_ENCODING || 'msgpack',
        maxCacheSize: process.env.STORAGE_MAX_CACHE_SIZE_MB || 500,
        moduleName: process.env.STORAGE_MODULE || '@hkube/fs-adapter'
    }
};
