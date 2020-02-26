const dataAdapter = require('./lib/adapters/data-adapter');
const DataRequest = require('./lib/communication/dataClient');
const DataServer = require('./lib/communication/dataServer');

module.exports = {
    dataAdapter,
    DataRequest,
    DataServer
};
