const Logger = require('@hkube/logger');
const log = Logger.GetLogFromContainer();
if (!log) {
    // init a new logger if not exist
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
