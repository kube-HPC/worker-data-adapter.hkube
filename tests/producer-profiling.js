const { Encoding } = require('@hkube/encoding');
const Logger = require('@hkube/logger');

const config = {
    transport: {
        console: true,
    },
    console: {
        json: false,
        colors: false,
        format: 'wrapper::{level}::{message}',
        level: process.env.HKUBE_LOG_LEVEL,
    },
    options: {
        throttle: {
            wait: 30000
        },
    },
};
new Logger('worker-data-adapter', config);
const ZMQProducer = require('../lib/communication/adapters/zeroMQ/streaming/ZMQProducer.js');
const encoding = new Encoding({ type: 'msgpack' });

const port = 4002;
const nodeName = 'A';
const consumerTypes = ['B', 'C'];
const messages = 5000;

const createProducer = () => {
    const producer = new ZMQProducer({
        port,
        maxMemorySize: 500 * 1024 * 1024,
        responseAccumulator: () => { },
        consumerTypes,
        encoding,
        nodeName
    });
    const flow = [{
        source: nodeName,
        next: consumerTypes
    }];
    const keys = Array.from(Array(messages).keys()).map(k => (encoding.encodeHeaderPayload({ data: `hello-${k + 1}` })));
    keys.forEach(k => producer.produce(k.header, k.payload, flow));
    console.log(`producer is ready with ${keys.length} messages in queue`);
    producer.start();
}

createProducer();