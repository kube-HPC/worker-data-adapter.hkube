const { expect } = require('chai');
const sinon = require('sinon');
const { Encoding } = require('@hkube/encoding');
const { DataRequest } = require('../lib/communication/data-client');
const DataServer = require('../lib/communication/data-server');
const consts = require('../lib/consts/messages');
const config = require('./config').discovery;
const { defaultStorage, storageAdapters } = require('./config');
const storage = storageAdapters[defaultStorage];
const encodingLib = new Encoding({ type: storage.encoding });

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
const data2 = Buffer.alloc(100);
const encoding = config.encoding;

const sleep = (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
}

describe('Getting data from by path', () => {
    let ds;
    afterEach('close sockets', () => {
        if (ds != null) {
            ds.close();
        }
        ds = null;
    })
    it('Getting data by task as json', async () => {
        ds = new DataServer(config);
        await ds.listen();
        const { header, payload } = encodingLib.encodeHeaderPayload(data1);
        ds.setSendingState(task1, payload, payload.length, header);
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, tasks: [task1], ...config });
        const response = await dr.invoke();
        const reply = response[0];
        expect(reply.size).to.eql(payload.length);
        expect(reply.content).to.eql(data1);
    });
    it('Getting data by task as binary', async () => {
        ds = new DataServer({ port: config.port, encoding });
        await ds.listen();
        const { header, payload } = encodingLib.encodeHeaderPayload(data2);
        ds.setSendingState(task1, payload, payload.length, header);
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, tasks: [task1], ...config });
        const response = await dr.invoke();
        const reply = response[0];
        expect(reply.size).to.eql(payload.length);
        expect(reply.content).to.eql(data2);
    });
    it('Getting data by multiple tasks', async () => {
        ds = new DataServer(config);
        await ds.listen();
        const { header, payload } = encodingLib.encodeHeaderPayload(data1);
        ds.setSendingState(task1, payload, payload.length, header);
        ds.setSendingState(task2, payload, payload.length, header);
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, tasks: [task1, task2], ...config });
        const response = await dr.invoke();
        const reply1 = response[0];
        const reply2 = response[1];
        expect(reply1.size).to.eql(payload.length);
        expect(reply1.content).to.eql(data1);
        expect(reply2.size).to.eql(payload.length);
        expect(reply2.content).to.eql(data1);
    });
    it('Failing data by multiple tasks', async () => {
        ds = new DataServer(config);
        await ds.listen();
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, tasks: [task1, task2], ...config });
        const response = await dr.invoke();
        const reply1 = response[0];
        const reply2 = response[1];
        expect(reply1.content.hkube_error.code).to.eql(consts.server.notAvailable);
        expect(reply2.content.hkube_error.code).to.eql(consts.server.notAvailable);
    });
    it('Failing to get data by task notAvailable', async () => {
        ds = new DataServer(config);
        await ds.listen();
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, tasks: [task1], ...config });
        const response = await dr.invoke();
        const reply = response[0];
        expect(reply.content.hkube_error.code).to.eql(consts.server.notAvailable);
    });
    it('Failing to get data by PingTimeout', async () => {
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, tasks: [task1], ...config });
        const response = await dr.invoke();
        const reply = response[0];
        expect(reply.content.hkube_error.code).to.eql(consts.requestType.ping.errorCode);
    });
    it('Failing to get data by RequestTimeout', async () => {
        ds = new DataServer(config);
        await ds.listen();
        const wrapper = async (args) => {
            await sleep(5000);
            return args;
        }
        sinon.replace(ds._adapter, 'send', wrapper);
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, tasks: [task1], ...config });
        const response = await dr.invoke();
        const reply = response[0];
        expect(reply.content.hkube_error.code).to.eql(consts.requestType.request.errorCode);
    });
    it('Check isServing', async () => {
        ds = new DataServer(config);
        await ds.listen();
        const serving1 = ds.isServing();
        const dr = new DataRequest({ address: { port: config.port, host: config.host }, tasks: [task1], ...config });
        await dr.invoke();
        const serving2 = ds.isServing();
        await sleep(10);
        expect(serving1).to.eql(false);
        expect(serving2).to.eq(true);
    });
});

