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


startMessageListening();