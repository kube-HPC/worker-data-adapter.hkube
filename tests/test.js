const chai = require('chai');
chai.use(require('chai-as-promised'))
const uuid = require('uuid/v4');
const expect = chai.expect
const { dataAdapter, DataServer } = require('../index.js');
const storageManager = require('@hkube/storage-manager');
const globalInput = [[3, 6, 9, 1, 5, 4, 8, 7, 2], 'asc'];

const storageFS = {
    baseDirectory: process.env.BASE_FS_ADAPTER_DIRECTORY || '/var/tmp/fs/storage',
    binary: !!process.env.STORAGE_BINARY
};

const config = {
    clusterName: process.env.CLUSTER_NAME || 'local',
    defaultStorage: process.env.DEFAULT_STORAGE || 'fs',
    enableCache: false,
    encoding: process.env.WORKER_ENCODING || 'bson',
    algorithmDiscovery: {
        host: process.env.POD_NAME || '127.0.0.1',
        port: process.env.DISCOVERY_PORT || 9020,
        encoding: process.env.WORKER_ENCODING || 'bson'
    },
    storageAdapters: {
        fs: {
            connection: storageFS,
            moduleName: process.env.STORAGE_MODULE || '@hkube/fs-adapter'
        }
    }
};

const { port } = config.algorithmDiscovery;
const dataServer = new DataServer({ port, encoding: config.encoding });

describe('Tests', () => {
    before(async () => {
        await dataAdapter.init(config);
    });
    describe('Storage', () => {
        it('should get data from storage and parse input data', async () => {
            const jobId = 'jobId:' + uuid();
            const link = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { data: { array: globalInput[0] } } });
            const link2 = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { myValue: globalInput[1] } });
            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { storageInfo: link, path: 'data.array' },
                'guid-6': { storageInfo: link2, path: 'myValue' }
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage });
            expect(result[0].data).to.eql(globalInput[0]);
            expect(result[1].prop).to.eql(globalInput[1]);
        });
        it('should get multiple data from storage and parse input data', async () => {
            const jobId = 'jobId:' + uuid();
            const link = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { data: { array: globalInput[0] } } });
            const link2 = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { myValue: globalInput[1] } });
            const input = [{ data: '$$guid-5' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': [
                    { storageInfo: link, path: 'data.array' },
                    { storageInfo: link, path: 'data.array.2' },
                    { storageInfo: link2, path: 'myValue' }
                ]
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage });
            expect(result[0].data[0]).to.eql(globalInput[0]);
            expect(result[0].data[1]).to.eql(globalInput[0][2]);
            expect(result[0].data[2]).to.eql(globalInput[1]);
        });
        it('should get data from storage by index and parse input data', async () => {
            const jobId = 'jobId:' + uuid();
            const link = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: globalInput[0] });
            const link2 = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: globalInput[1] });
            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { storageInfo: link, path: '4' },
                'guid-6': { storageInfo: link2, path: '2' }
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage });
            expect(result[0].data).to.eql(globalInput[0][4]);
            expect(result[1].prop).to.eql(globalInput[1][2]);
        });
        it('should get data from storage by path and index and parse input data', async () => {
            const jobId = 'jobId:' + uuid();
            const link = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { data: { array: globalInput[0] } } });
            const link2 = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { myValue: globalInput[1] } });
            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { storageInfo: link, path: 'data.array.4' },
                'guid-6': { storageInfo: link2, path: 'myValue.2' }
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage });
            expect(result[0].data).to.eql(globalInput[0][4]);
            expect(result[1].prop).to.eql(globalInput[1][2]);
        });
        it('should fail to get data from storage by path', async () => {
            const jobId = 'jobId:' + uuid();
            const taskId = 'taskId:' + uuid();
            const link = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { data: { array: globalInput[0] } } });
            const link2 = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { myValue: globalInput[1] } });
            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { storageInfo: link, taskId, path: 'no_such' },
                'guid-6': { storageInfo: link2, taskId, path: 'no_such' }
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage });
            expect(result[0].data).to.eql(undefined);
            expect(result[1].prop).to.eql(undefined);
        });
        it('should set data to storage', async () => {
            const jobId = 'jobId:' + uuid();
            const taskId = 'taskId:' + uuid();
            const result = await dataAdapter.setData({ jobId, taskId, data: globalInput[0] });
            expect(result).to.have.property('path');
        });
        it.skip('should create storage path', async () => {
            const jobId = 'jobId:' + uuid();
            const taskId = 'taskId:' + uuid();
            const result = dataAdapter.createStoragePath({ jobId, taskId });
            expect(result).to.contain(jobId);
            expect(result).to.contain(taskId);
        });
        it('should create metadata', async () => {
            const data = { array: globalInput[0], myValue: { prop: globalInput[1] } }
            const result = dataAdapter.createMetadata({ nodeName: 'green', data, savePaths: ['green.array', 'green.myValue.prop'] });
            expect(result['green.array']).to.eql({ type: 'array', size: globalInput[0].length });
            expect(result['green.myValue.prop']).to.eql({ type: 'string' });
        });
        it('should create metadata', async () => {
            const size = 100000;
            const data = Array.from(Array(size).keys());
            const result = dataAdapter.createMetadata({ nodeName: 'green', data, savePaths: ['green'] });
            expect(result['green']).to.eql({ type: 'array', size });
        });
    });
    describe('Server', () => {
        it('should get data from server and parse input data', async () => {
            const taskId = 'taskId:' + uuid();
            dataServer.setSendingState(taskId, { data: { array: globalInput[0] }, myValue: globalInput[1] });
            const discovery = config.algorithmDiscovery;
            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { discovery, taskId, path: 'data.array' },
                'guid-6': { discovery, taskId, path: 'myValue' }
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage });
            expect(result[0].data).to.eql(globalInput[0]);
            expect(result[1].prop).to.eql(globalInput[1]);
        });
        it('should get multiple data from server and parse input data', async () => {
            const taskId = 'taskId:' + uuid();
            dataServer.setSendingState(taskId, { data: { array: globalInput[0] }, myValue: globalInput[1] });
            const discovery = config.algorithmDiscovery;
            const input = [{ data: '$$guid-5' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': [
                    { discovery, taskId, path: 'data.array' },
                    { discovery, taskId, path: 'data.array.2' },
                    { discovery, taskId, path: 'myValue' }
                ]
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage });
            expect(result[0].data[0]).to.eql(globalInput[0]);
            expect(result[0].data[1]).to.eql(globalInput[0][2]);
            expect(result[0].data[2]).to.eql(globalInput[1]);
        });
        it('should get data from storage by index and parse input data', async () => {
            const taskId = 'taskId:' + uuid();
            const discovery = config.algorithmDiscovery;
            dataServer.setSendingState(taskId, globalInput[0]);
            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { discovery, taskId, path: '2' },
                'guid-6': { discovery, taskId, path: '4' }
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage });
            expect(result[0].data).to.eql(globalInput[0][2]);
            expect(result[1].prop).to.eql(globalInput[0][4]);
        });
        it('should get data from server by path and index and parse input data', async () => {
            const taskId = 'taskId:' + uuid();
            dataServer.setSendingState(taskId, { data: { array: globalInput[0] }, myValue: globalInput[1] });
            const discovery = config.algorithmDiscovery;
            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { discovery, taskId, path: 'data.array.4' },
                'guid-6': { discovery, taskId, path: 'myValue.2' }
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage });
            expect(result[0].data).to.eql(globalInput[0][4]);
            expect(result[1].prop).to.eql(globalInput[1][2]);
        });
        it('should fail to get data from server by path', async () => {
            const taskId = 'taskId:' + uuid();
            dataServer.setSendingState(taskId, { data: { array: globalInput[0] }, myValue: globalInput[1] });
            const discovery = config.algorithmDiscovery;
            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { discovery, taskId, path: 'no_such' },
                'guid-6': { discovery, taskId, path: 'no_such' }
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage });
            expect(result[0].data).to.eql(undefined);
            expect(result[1].prop).to.eql(undefined);
        });
    });
    describe('Storage and Server', () => {
        it('should fail to get data from server and get from storage instead', async () => {
            const jobId = 'jobId:' + uuid();
            const link = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { data: { array: globalInput[0] } } });
            const link2 = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { myValue: globalInput[1] } });

            const { host, encoding } = config.algorithmDiscovery;
            const discovery = { host, encoding, port: 5090 };

            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { discovery, storageInfo: link, path: 'data.array' },
                'guid-6': { discovery, storageInfo: link2, path: 'myValue' }
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage });
            expect(result[0].data).to.eql(globalInput[0]);
            expect(result[1].prop).to.eql(globalInput[1]);
        });
        it('should fail to find storage key inside input', async () => {
            const input = ['$$guid-5'];
            const storage = { 'guid-no_such': { storageInfo: 'bla' } };
            const flatInput = dataAdapter.flatInput({ input, storage });
            await expect(dataAdapter.getData({ input, flatInput, storage })).to.be.rejectedWith(Error, 'unable to find storage key');
        });
    });
});
