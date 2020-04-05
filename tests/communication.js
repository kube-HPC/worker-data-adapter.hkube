const { expect } = require('chai');
const { DataRequest } = require('../lib/communication/dataClient');
const DataServer = require('../lib/communication/dataServer');
const consts = require('../lib/consts/messages').server;
const config = {
    host: process.env.POD_NAME || '127.0.0.1',
    port: process.env.DISCOVERY_PORT || 9020,
    encoding: process.env.WORKER_ENCODING || 'bson'
}
const task1 = 'task_1';
const task2 = 'task_2';
const data1 = {
    level1: {
        level2: {
            value1: 'l1_l2_value_1',
            value2: 'l1_l2_value_2',
        },
        value1: 'l1_value_1'
    },
    value1: 'value_1'
};
const data2 = {
    level1: {
        level2: {
            value1: 'd2_l1_l2_value_1',
            value2: 'd2_l1_l2_value_2',
        },
        value1: 'd2_l1_value_1'
    },
    value1: 'd2_value_1'
};
const data3 = new Buffer(1024 * 1024 * 100);
const sleep = (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
}

const encoding = config.encoding;

describe('Getting data from by path', () => {
    let ds;
    let dr;
    afterEach('close sockets', () => {
        if (ds != null) {
            ds.close();
        }
    })
    it('Getting data by path as json', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data1);
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: 'level1', encoding });
        const reply = await dr.invoke();
        expect(reply.data.level2.value1).eq('l1_l2_value_1');
    });
    it('Getting data by path as binary', async () => {
        ds = new DataServer({ port: config.port, encoding });
        await ds.listen();
        ds.setSendingState(task1, data1);
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: 'level1', encoding });
        const reply = await dr.invoke();
        expect(reply.data.level2.value1).eq('l1_l2_value_1');
    });
    it('Getting complete data', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data1);
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, encoding });
        const reply = await dr.invoke();
        expect(reply.data.level1.level2.value1).eq('l1_l2_value_1');
    });
    xit('Getting big data', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data3);
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, encoding });
        const reply = await dr.invoke();
        expect(reply.message).eq(consts.success);

    }).timeout(6000);
    it('Getting data after taskId changed', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data1);
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: 'level1', encoding });
        let reply = await dr.invoke();
        expect(reply.data.level2.value1).eq('l1_l2_value_1');
        ds.setSendingState(task2, data2);
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task2, dataPath: 'level1', encoding });
        reply = await dr.invoke();
        expect(reply.data.level2.value1).eq('d2_l1_l2_value_1');
    });
    it('Failing to get data with old taskId', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data1);
        ds.setSendingState(task2, data2);
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: 'level1', encoding });
        reply = await dr.invoke();
        expect(reply.error.code).eq(consts.notAvailable);
        expect(reply.error.message).eq(`Current taskId is ${task2}`);
    });
    it('Failing to get data when sending ended', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data1);
        ds.endSendingState();
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: 'level1', encoding });
        reply = await dr.invoke();
        expect(reply.error.code).eq(consts.notAvailable);
        expect(reply.error.message).eq(`Current taskId is null`);
    });
    it('Failing to get data in path that does not exist', async () => {
        ds = new DataServer(config);
        await ds.listen();
        ds.setSendingState(task1, data1);
        const noneExisting = 'noneExisting';
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: noneExisting, encoding });
        const reply = await dr.invoke();
        expect(reply.error.code).eq(consts.noSuchDataPath);
        expect(reply.error.message).eq(`${noneExisting} does not exist in data`);
    });
    it('Timing out when there is no server side', async () => {
        const noneExisting = 'noneExisting';
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: noneExisting, encoding });
        ds = null;
        const reply = await dr.invoke();
        expect(reply.error.code).eq(consts.notAvailable);
        expect(reply.error.message).eq(`server ${config.host}:${config.port} unreachable`);
    });
    it.skip('Check number of active connections', async () => {
        ds = new DataServer(config);
        await ds.listen();

        const wrapper = (fn) => {
            const inner = async (...args) => {
                await sleep(100);
                return fn(...args);
            }
            return inner;
        }
        ds._encoding.decode = wrapper(ds._encoding.decode.bind(ds._encoding));

        const noneExisting = 'noneExisting';
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: noneExisting, encoding });
        dr.invoke();
        dr = new DataRequest({ address: { port: config.port, host: config.host }, taskId: task1, dataPath: noneExisting, encoding });
        dr.invoke();
        await sleep(10);
        expect(ds.isServing()).eq(true);
        await sleep(500);
        expect(ds.isServing()).eq(false);
    });
});

