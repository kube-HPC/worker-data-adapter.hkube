const Logger = require('@hkube/logger');
const log = Logger.GetLogFromContainer();
if (!log) {
    // init a new logger if not exist
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
    new Logger('worker-data-adapter', config); // eslint-disable-line
}

const dataAdapter = require('./lib/adapters/data-adapter');
const DataRequest = require('./lib/communication/data-client');
const StreamingManager = require('./lib/communication/streaming/StreamingManager');
const DataServer = require('./lib/communication/data-server');
const Events = require('./lib/consts/events');

module.exports = {
    dataAdapter,
    DataRequest,
    StreamingManager,
    DataServer,
    Events
};
