const chai = require('chai');
chai.use(require('chai-as-promised'))
const uuid = require('uuid/v4');
const clone = require('clone');
const { Encoding } = require('@hkube/encoding');
const storageManager = require('@hkube/storage-manager');
const expect = chai.expect
const { dataAdapter, DataServer } = require('../index.js');
const config = require('./config');
const discovery = { ...config.discovery, port: 9500 };
const storage = config.storageAdapters[config.defaultStorage];
const dataServer = new DataServer(discovery);
const encodingLib = new Encoding({ type: storage.encoding });
const globalInput = [[3, 6, 9, 1, 5, 4, 8, 7, 2], 'asc'];
const serverData = { data: { array: globalInput[0] }, myValue: globalInput[1] }
const mainInput = [{ data: '$$guid-5' }, { prop: '$$guid-6' }, 'test-param', true, 12345];

describe('Tests', () => {
    before(async () => {
        await dataAdapter.init(config);
        await dataServer.listen();
    });
    describe('test limits', () => {
        it.skip('number of sockets', async () => {
            const count = 2000
            const requests = [];
            for (let i = 0; i < count; i++) {
                requests.push(dataAdapter._getFromPeer({
                    discovery: { host: '127.0.0.1', port: 19200 + i }, tasks: ['t1'], taskId: 't2'
                }))
            }
            await Promise.all(requests)
        });
    });
    describe('flatInput', () => {
        it('should get data without input', async () => {
            const input = clone(mainInput);
            const storage = {};
            const flatInput = dataAdapter.flatInput({ input, storage });
            expect(flatInput).to.eql({});
        });
        it('should get data without storage', async () => {
            const input = null;
            const storage = {
                'guid-5': { path: 'data.array' },
                'guid-6': { path: 'myValue' }
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            expect(flatInput).to.eql({});
        });
        it('should get data with input and storage', async () => {
            const input = clone(mainInput);
            const storage = {
                'guid-5': { path: 'data.array' },
                'guid-6': { path: 'myValue' }
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            expect(flatInput).to.eql({ '0.data': '$$guid-5', '1.prop': '$$guid-6', '2': 'test-param', '3': true, '4': 12345 });
        });
        it('should get data without flat input', async () => {
            const input = clone(mainInput);
            const storage = {};
            const result = await dataAdapter.getData({ input, storage });
            expect(result).to.eql(input);
        });
    });
    describe('Storage', () => {
        it('should get data from storage and parse input data', async () => {
            const jobId = 'jobId:' + uuid();
            const link = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { data: { array: globalInput[0] } } });
            const link2 = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: { myValue: globalInput[1] } });
            const input = clone(mainInput);
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
            result[0].data.sort();
            expect(result[0].data[0]).to.eql(globalInput[0]);
            expect(result[0].data[1]).to.eql(globalInput[0][2]);
            expect(result[0].data[2]).to.eql(globalInput[1]);
        });
        it('should get data from storage by index and parse input data', async () => {
            const jobId = 'jobId:' + uuid();
            const link = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: globalInput[0] });
            const link2 = await storageManager.hkube.put({ jobId, taskId: 'taskId:' + uuid(), data: globalInput[1] });
            const input = clone(mainInput);
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
            const input = clone(mainInput);
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
            const input = clone(mainInput);
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
            const result = await dataAdapter.setData({ jobId, taskId, data: globalInput[0].toString() });
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
        it('should create metadata', async () => {
            const size = 100000;
            const data = Array.from(Array(size).keys());
            const result = dataAdapter.createMetadata({ nodeName: 'green', data, savePaths: ['green'] });
            expect(result['green']).to.eql({ type: 'array', size });
        });
    });
    describe('Server', () => {
        it('should get data from local server', async () => {
            const taskId = 'taskId:' + uuid();
            const { header, payload } = encodingLib.encodeHeaderPayload(serverData);
            dataServer.setSendingState(taskId, payload, payload.length, header);
            const input = clone(clone(mainInput));
            const storage = {
                'guid-5': { discovery, taskId, path: 'data.array' },
                'guid-6': { discovery, taskId, path: 'myValue' }
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            dataAdapter._dataServer = dataServer;
            const result = await dataAdapter.getData({ input, flatInput, storage });
            dataAdapter._dataServer = null;
            expect(result[0].data).to.eql(globalInput[0]);
            expect(result[1].prop).to.eql(globalInput[1]);
        });
        it('should get data from server and parse input data', async () => {
            const taskId = 'taskId:' + uuid();
            const { header, payload } = encodingLib.encodeHeaderPayload(serverData);
            dataServer.setSendingState(taskId, payload, payload.length, header);
            const input = clone(clone(mainInput));
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
            const jobId = 'jobId:' + uuid();
            const taskId = 'taskId:' + uuid();
            const { header, payload } = encodingLib.encodeHeaderPayload(serverData);
            dataServer.setSendingState(taskId, payload, payload.length, header);
            const input = [{ data: '$$guid-5' }, 'test-param', true, 12345];
            const storage = {
                'guid-5': [
                    { discovery, tasks: [taskId], path: 'data.array' },
                    { discovery, tasks: [taskId], path: 'data.array.2' },
                    { discovery, tasks: [taskId], path: 'myValue' }
                ]
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage, jobId });
            result[0].data.sort();
            expect(result[0].data[0]).to.eql(globalInput[0]);
            expect(result[0].data[1]).to.eql(globalInput[0][2]);
            expect(result[0].data[2]).to.eql(globalInput[1]);
        });
        it('should get multiple data without a server and parse input data', async () => {
            const jobId = 'jobId:' + uuid();
            const taskId = 'taskId:' + uuid();
            await storageManager.hkube.put({ jobId, taskId, data: serverData });
            const { header, payload } = encodingLib.encodeHeaderPayload(serverData);
            dataServer.setSendingState(taskId, payload, payload.length, header);
            const input = [{ data: '$$guid-5' }, 'test-param', true, 12345];
            const discovery2 = { ...discovery, port: 5090 };
            const storage = {
                'guid-5': [
                    { discovery: discovery2, tasks: [taskId], path: 'myValue' }
                ]
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ input, flatInput, storage, jobId });
            expect(result[0].data[0]).to.eql(globalInput[1]);
        });
        it('should get data from storage by index and parse input data', async () => {
            const taskId = 'taskId:' + uuid();
            const { header, payload } = encodingLib.encodeHeaderPayload(globalInput[0]);
            dataServer.setSendingState(taskId, payload, payload.length, header);
            const input = clone(mainInput);
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
            const { header, payload } = encodingLib.encodeHeaderPayload(serverData);
            dataServer.setSendingState(taskId, payload, payload.length, header);
            const input = clone(mainInput);
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
            const { header, payload } = encodingLib.encodeHeaderPayload(serverData);
            dataServer.setSendingState(taskId, payload, payload.length, header);
            const input = clone(mainInput);
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
            const taskId1 = 'taskId:' + uuid();
            const taskId2 = 'taskId:' + uuid();
            const link = await storageManager.hkube.put({ jobId, taskId: taskId1, data: { data: { array: globalInput[0] } } });
            const link2 = await storageManager.hkube.put({ jobId, taskId: taskId2, data: { myValue: globalInput[1] } });
            const discovery2 = { ...discovery, port: 5090 };
            const input = clone(mainInput);
            const storage = {
                'guid-5': { discovery: discovery2, taskId: taskId1, storageInfo: link, path: 'data.array' },
                'guid-6': { discovery: discovery2, taskId: taskId2, storageInfo: link2, path: 'myValue' }
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
        it('should fail to get data from server and get from storage instead', async () => {
            const jobId = 'jobId:' + uuid();
            const taskId1 = 'taskId:' + uuid();
            const taskId2 = 'taskId:' + uuid();
            const data1 = { data: { array: globalInput[0] } };
            const data2 = { myValue: globalInput[1] };
            const { header: header1, payload: payload1 } = encodingLib.encodeHeaderPayload(data1);
            const { header: header2, payload: payload2 } = encodingLib.encodeHeaderPayload(data2);
            dataServer.setSendingState(taskId1, payload1, payload1.length, header1);
            dataServer.setSendingState(taskId2, payload2, payload2.length, header2);
            await storageManager.hkube.put({ jobId, taskId: taskId1, data: data1 });
            await storageManager.hkube.put({ jobId, taskId: taskId2, data: data2 });
            const input = clone(mainInput);
            const storage = {
                'guid-5': [{ discovery, tasks: [taskId1, taskId1], path: 'data.array' }],
                'guid-6': [{ discovery, tasks: [taskId1, taskId2], path: 'myValue' }]
            };
            const flatInput = dataAdapter.flatInput({ input, storage });
            const result = await dataAdapter.getData({ jobId, input, flatInput, storage });
            expect(result[0].data).to.eql([globalInput[0], globalInput[0]]);
            expect(result[1].prop).to.eql([undefined, globalInput[1]]);
        });
    });
    describe('createStorageInfo', () => {
        it('should get data from storage and parse input data', async () => {
            const jobId = 'jobId:' + uuid();
            const taskId = 'taskId:' + uuid();
            const nodeName = 'green';
            const encodedData = dataAdapter.encode(serverData);
            const info = dataAdapter.createStorageInfo({ jobId, taskId, nodeName, data: serverData, encodedData, savePaths: ['green'] });
            expect(info).to.have.property('storageInfo');
            expect(info).to.have.property('metadata');
            expect(info.storageInfo).to.have.property('path');
            expect(info.storageInfo).to.have.property('size');
        });

    });
});
