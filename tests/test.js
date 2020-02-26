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
    algorithmDiscovery: {
        host: process.env.POD_NAME || '127.0.0.1',
        port: process.env.DISCOVERY_PORT || 9020,
        binary: true
    },
    storageAdapters: {
        fs: {
            connection: storageFS,
            moduleName: process.env.STORAGE_MODULE || '@hkube/fs-adapter'
        }
    }
};

const { port, binary } = config.algorithmDiscovery;
const dataServer = new DataServer({ port, binary });

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
            const result = await dataAdapter.getData(input, storage);
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
            const result = await dataAdapter.getData(input, storage);
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
                'guid-5': { storageInfo: link, index: 4 },
                'guid-6': { storageInfo: link2, index: 2 }
            };
            const result = await dataAdapter.getData(input, storage);
            expect(result[0].data).to.eql(globalInput[0][4]);
            expect(result[1].prop).to.eql(globalInput[1][2]);
        });
        it('should get data from storage by path and index and parse input data', async () => {
            const jobId = 'jobId:' + uuid();
            const link = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { data: { array: globalInput[0] } } });
            const link2 = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { myValue: globalInput[1] } });
            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { storageInfo: link, path: 'data.array', index: 4 },
                'guid-6': { storageInfo: link2, path: 'myValue', index: 2 }
            };
            const result = await dataAdapter.getData(input, storage);
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
            const result = await dataAdapter.getData(input, storage);
            expect(result[0].data).to.eql(undefined);
            expect(result[1].prop).to.eql(undefined);
        });
        it('should set data to storage', async () => {
            const jobId = 'jobId:' + uuid();
            const taskId = 'taskId:' + uuid();
            const result = await dataAdapter.setData({ jobId, taskId, data: globalInput[0] });
            expect(result).to.have.property('path');
        });
        it('should create storage path', async () => {
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
    });
    describe('Server', () => {
        it('should get data from server and parse input data', async () => {
            const taskId = 'taskId:' + uuid();
            const { host, port } = config.algorithmDiscovery;
            dataServer.setSendingState(taskId, { data: { array: globalInput[0] }, myValue: globalInput[1] });
            const discovery = { host, port };
            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { discovery, taskId, path: 'data.array' },
                'guid-6': { discovery, taskId, path: 'myValue' }
            };
            const result = await dataAdapter.getData(input, storage);
            expect(result[0].data).to.eql(globalInput[0]);
            expect(result[1].prop).to.eql(globalInput[1]);
        });
        it('should get multiple data from server and parse input data', async () => {
            const taskId = 'taskId:' + uuid();
            const { host, port } = config.algorithmDiscovery;
            dataServer.setSendingState(taskId, { data: { array: globalInput[0] }, myValue: globalInput[1] });
            const discovery = { host, port };
            const input = [{ data: '$$guid-5' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': [
                    { discovery, taskId, path: 'data.array' },
                    { discovery, taskId, path: 'data.array.2' },
                    { discovery, taskId, path: 'myValue' }
                ]
            };
            const result = await dataAdapter.getData(input, storage);
            expect(result[0].data[0]).to.eql(globalInput[0]);
            expect(result[0].data[1]).to.eql(globalInput[0][2]);
            expect(result[0].data[2]).to.eql(globalInput[1]);
        });
        it('should get data from storage by index and parse input data', async () => {
            const taskId = 'taskId:' + uuid();
            const { host, port } = config.algorithmDiscovery;
            dataServer.setSendingState(taskId, globalInput[0]);
            const discovery = { host, port };
            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { discovery, taskId, index: 2 },
                'guid-6': { discovery, taskId, index: 4 }
            };
            const result = await dataAdapter.getData(input, storage);
            expect(result[0].data).to.eql(globalInput[0][2]);
            expect(result[1].prop).to.eql(globalInput[0][4]);
        });
        it('should get data from server by path and index and parse input data', async () => {
            const taskId = 'taskId:' + uuid();
            const { host, port } = config.algorithmDiscovery;
            dataServer.setSendingState(taskId, { data: { array: globalInput[0] }, myValue: globalInput[1] });
            const discovery = { host, port };
            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { discovery, taskId, path: 'data.array', index: 4 },
                'guid-6': { discovery, taskId, path: 'myValue', index: 2 }
            };
            const result = await dataAdapter.getData(input, storage);
            expect(result[0].data).to.eql(globalInput[0][4]);
            expect(result[1].prop).to.eql(globalInput[1][2]);
        });
        it('should fail to get data from server by path', async () => {
            const taskId = 'taskId:' + uuid();
            const { host, port } = config.algorithmDiscovery;
            dataServer.setSendingState(taskId, { data: { array: globalInput[0] }, myValue: globalInput[1] });
            const discovery = { host, port };

            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { discovery, taskId, path: 'no_such' },
                'guid-6': { discovery, taskId, path: 'no_such' }
            };
            const result = await dataAdapter.getData(input, storage);
            expect(result[0].data).to.eql(undefined);
            expect(result[1].prop).to.eql(undefined);
        });
    });
    describe('Storage and Server', () => {
        it('should fail to get data from server and get from storage instead', async () => {
            const jobId = 'jobId:' + uuid();
            const taskId = 'taskId:' + uuid();
            const link = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { data: { array: globalInput[0] } } });
            const link2 = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { myValue: globalInput[1] } });

            const { host } = config.algorithmDiscovery;
            const discovery = { host, port: 5090 };

            const input = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': { discovery, storageInfo: link, path: 'data.array' },
                'guid-6': { discovery, storageInfo: link2, path: 'myValue' }
            };
            const result = await dataAdapter.getData(input, storage);
            expect(result[0].data).to.eql(globalInput[0]);
            expect(result[1].prop).to.eql(globalInput[1]);
        });
        it('should fail to find storage key inside input', async () => {
            const input = ['$$guid-5'];
            const storage = { 'guid-no_such': { storageInfo: 'bla' } };
            await expect(dataAdapter.getData(input, storage)).to.be.rejectedWith(Error, 'unable to find storage key');
        });
    });
});
