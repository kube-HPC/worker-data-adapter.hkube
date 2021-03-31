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
