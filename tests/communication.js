const { expect } = require('chai');
const { DataRequest } = require('../lib/communication/data-client');
const DataServer = require('../lib/communication/data-server');
const consts = require('../lib/consts/messages').server;
const config = require('./config').discovery;
const task1 = 'task_1';
const task2 = 'task_2';
let data1 = {
    level1: {
        level2: {
            value1: 'l1_l2_value_1',
            value2: 'l1_l2_value_2',
        },
        value1: 'l1_value_1'
    },
    value1: 'value_1'
};
let data2 = {
    level1: {
        level2: {
            value1: 'd2_l1_l2_value_1',
            value2: 'd2_l1_l2_value_2',
        },
        value1: 'd2_l1_value_1'
    },
    value1: 'd2_value_1'
};
const data3 = Buffer.alloc(100);

const encoding = config.encoding;

const sleep = (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
}

describe('Getting data from by path', () => {
    let ds;
    afterEach('close sockets', () => {
        if (ds != null && !ds._adapter._responder.closed) {
            ds.close();
        }
    })
    it('Getting data by path as json', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data1);
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: 'level1', encoding });
        const reply = await dr.invoke();
        expect(reply.level2.value1).eq('l1_l2_value_1');
    });
    it('Getting data by path as binary', async () => {
        ds = new DataServer({ port: config.port, encoding });
        await ds.listen();
        ds.setSendingState(task1, data1);
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: 'level1', encoding });
        const reply = await dr.invoke();
        expect(reply.level2.value1).eq('l1_l2_value_1');
    });
    it('Getting complete data', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data1);
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, encoding });
        const reply = await dr.invoke();
        expect(reply.level1.level2.value1).eq('l1_l2_value_1');
    });
    it('Getting big data', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data3);
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, encoding });
        const startTime = new Date().getTime();
        const reply = await dr.invoke();
        const endTime = new Date().getTime();
        console.log('time:' + (endTime - startTime))
        expect(reply).eql(data3);

    });
    it('Getting data after taskId changed', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data1);
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: 'level1', encoding });
        let reply = await dr.invoke();
        expect(reply.level2.value1).eq('l1_l2_value_1');
        ds.setSendingState(task2, data2);
        const dr2 = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task2, dataPath: 'level1', encoding });
        reply = await dr2.invoke();
        expect(reply.level2.value1).eq('d2_l1_l2_value_1');
    });
    it('Failing to get data with old taskId', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data1);
        ds.setSendingState(task2, data2);
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: 'level1', encoding });
        reply = await dr.invoke();
        expect(reply).eql(data1.level1);
    });
    it.skip('Disconnect during invoke', async () => {
        ds = new DataServer(config);
        await ds.listen();
        const wrapper = (fn) => {
            const inner = async (...args) => {
                await sleep(500);
                return fn(...args);
            }
            return inner;
        }
        ds._encoding.decode = wrapper(ds._encoding.decode.bind(ds._encoding));
        const noneExisting = 'noneExisting';
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: noneExisting, encoding });
        replyPromise = dr.invoke();
        ds.close();
        reply = await replyPromise;

        expect(reply.hkube_error.code).eq(consts.unknown);
        expect(reply.hkube_error.message).eq('early disconnect');
    });
    it('Failing to get data in path that does not exist', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data1);
        const noneExisting = 'noneExisting';
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: noneExisting, encoding });
        const reply = await dr.invoke();
        expect(reply.hkube_error.code).eq(consts.noSuchDataPath);
        expect(reply.hkube_error.message).eq(`${noneExisting} does not exist in data`);
    });
    it('Timing out when there is no server side', async () => {
        const noneExisting = 'noneExisting';
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: noneExisting, encoding });
        ds = null;
        const reply = await dr.invoke();
        expect(reply.hkube_error.code).eq(consts.notAvailable);
        expect(reply.hkube_error.message).eq(`server ${config.host}:${config.port} unreachable`);
    });
    it.skip('Check number of active connections', async () => {
        ds = new DataServer(config);
        await ds.listen();

        const noneExisting = 'noneExisting';
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: noneExisting, encoding });
        dr.invoke();
        const dr2 = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: noneExisting, encoding });
        dr2.invoke();
        await sleep(10);
        expect(ds.isServing()).eq(true);
        await sleep(150);
        expect(ds.isServing()).eq(false);
    });
    it.skip('Check waitTill Done serving', async () => {
        ds = new DataServer(config);
        await ds.listen();

        const noneExisting = 'noneExisting';
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: noneExisting, encoding });
        dr.invoke();
        const dr2 = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: noneExisting, encoding });
        dr2.invoke();
        await sleep(10);
        expect(ds.isServing()).eq(true);
        await ds.waitTillServingIsDone();
        expect(ds.isServing()).eq(false);
    });
});

