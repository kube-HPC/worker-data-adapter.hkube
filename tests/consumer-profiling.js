const { Encoding } = require('@hkube/encoding');
const now = require('performance-now')
const Logger = require('@hkube/logger');
const { sleep } = require('../lib/utils/waitFor');
const config = {
    isDefault: true,
    enableColors: false,
    format: 'wrapper::{level}::{message}',
    verbosityLevel: process.env.HKUBE_LOG_LEVEL || 2,
    throttle: {
        wait: 30000
    },
    transport: {
        console: true,
    },
};
new Logger('worker-data-adapter', config);
const ZMQListener = require('../lib/communication/adapters/zeroMQ/streaming/ZMQListener.js');
const encoding = new Encoding({ type: 'msgpack' });

const port = 4002;
const consumerTypes = ['B', 'C'];
const remoteAddress = `tcp://localhost:${port}`;


const startMessageListening = async () => {
    let i = 0;
    const listeners = [];
    consumerTypes.forEach(c => {
        const keys = Array.from(Array(10).keys());
        keys.forEach(k => {
            const listener = new ZMQListener({
                remoteAddress,
                consumerName: c,
                encoding,
                onMessage: async (msgFlow, header, value) => {
                    i += 1;
                    const payload = encoding.decodeHeaderPayload(header, value);
                    console.log(`handling message ${i} ${payload.data}`);
                    // await sleep(5)
                }
            });
            listeners.push(listener);
        });
    });

    while (true) {
        for (const listener of listeners) {
            await listener.fetch();
        }
    }
}

const start = now();
const end = now();
const duration = parseFloat((end - start).toFixed(4));


startMessageListening();