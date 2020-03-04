const { expect } = require('chai');
const DataRequest = require('../lib/communication/dataClient');
const DataServer = require('../lib/communication/dataServer');
const consts = require('../lib/consts/messages').server;
const config = {
    port: 3003,
    host: 'localhost'
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
}


describe('Getting data from by path', () => {
    let ds;
    let dr;
    afterEach('close sockets', () => {
        if (ds != null) {
            ds.close();
        }
    })
    it('Getting data by path as json', async () => {
        ds = new DataServer({ port: config.port });
        ds.setSendingState(task1, data1);
        dr = new DataRequest({ port: config.port, host: config.host, taskId: task1, dataPath: 'level1' });
        const reply = await dr.invoke();
        expect(reply.message).eq(consts.success);
        expect(reply.data.level2.value1).eq('l1_l2_value_1');
    });
    it('Getting data by path as binary', async () => {
        ds = new DataServer({ port: config.port, binary: true });
        ds.setSendingState(task1, data1);
        dr = new DataRequest({ port: config.port, host: config.host, taskId: task1, dataPath: 'level1', binary: true });
        const reply = await dr.invoke();
        expect(reply.message).eq(consts.success);
        expect(reply.data.level2.value1).eq('l1_l2_value_1');
    });
    it('Getting complete data', async () => {
        ds = new DataServer({ port: config.port });
        ds.setSendingState(task1, data1);
        dr = new DataRequest({ port: config.port, host: config.host, taskId: task1 });
        const reply = await dr.invoke();
        expect(reply.message).eq(consts.success);
        expect(reply.data.level1.level2.value1).eq('l1_l2_value_1');
    });
    it('Getting data after taskId changed', async () => {
        ds = new DataServer({ port: config.port });
        ds.setSendingState(task1, data1);
        dr = new DataRequest({ port: config.port, host: config.host, taskId: task1, dataPath: 'level1' });
        let reply = await dr.invoke();
        expect(reply.message).eq(consts.success);
        expect(reply.data.level2.value1).eq('l1_l2_value_1');
        ds.setSendingState(task2, data2);
        dr = new DataRequest({ port: config.port, host: config.host, taskId: task2, dataPath: 'level1' });
        reply = await dr.invoke();
        expect(reply.message).eq(consts.success);
        expect(reply.data.level2.value1).eq('d2_l1_l2_value_1');
    });
    it('Failing to get data with old taskId', async () => {
        ds = new DataServer({ port: config.port });
        ds.setSendingState(task1, data1);
        ds.setSendingState(task2, data2);
        dr = new DataRequest({ port: config.port, host: config.host, taskId: task1, dataPath: 'level1' });
        reply = await dr.invoke();
        expect(reply.message).eq(consts.notAvailable);
        expect(reply.reason).eq(`Current taskId is ${task2}`);
    });
    it('Failing to get data when sending ended', async () => {
        ds = new DataServer({ port: config.port });
        ds.setSendingState(task1, data1);
        ds.endSendingState();
        dr = new DataRequest({ port: config.port, host: config.host, taskId: task1, dataPath: 'level1' });
        reply = await dr.invoke();
        expect(reply.message).eq(consts.notAvailable);
        expect(reply.reason).eq(`Current taskId is null`);
    });

    it('Failing to get data in path that does not exist', async () => {
        ds = new DataServer({ port: config.port });
        ds.setSendingState(task1, data1);
        const noneExisting = 'noneExisting';
        dr = new DataRequest({ port: config.port, host: config.host, taskId: task1, dataPath: noneExisting });
        const reply = await dr.invoke();
        expect(reply.error).eq(consts.noSuchDataPath);
        expect(reply.reason).eq(`${noneExisting} does not exist in data`);
    });

    it('Timing out when there is no server side', async () => {
        const noneExisting = 'noneExisting';
        dr = new DataRequest({ port: config.port, host: config.host, taskId: task1, dataPath: noneExisting });
        ds = null;
        const reply = await dr.invoke();
        expect(reply.message).eq(consts.notAvailable);
        expect(reply.reason).eq(`server ${config.host}:${config.port} unreachable`);
    });
    it.only('Check number of active connections', async () => {
        ds = new DataServer({ port: config.port });
        function sleep(ms) {
            return new Promise(resolve => setTimeout(resolve, ms));
        }
        ds._parse = async (a) => { await sleep(100); return dr._parse(a) };
        const noneExisting = 'noneExisting';
        dr = new DataRequest({ port: config.port, host: config.host, taskId: task1, dataPath: noneExisting });
        dr.invoke();
        dr = new DataRequest({ port: config.port, host: config.host, taskId: task1, dataPath: noneExisting });
        dr.invoke();
        await sleep(10);
        expect(ds.isServing()).eq(true);
        await sleep(201);
        expect(ds.isServing()).eq(false);
    });
}
);

