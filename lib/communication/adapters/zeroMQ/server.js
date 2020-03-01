const zmq = require('zeromq');
const EventEmitter = require('events');

class ZeroMQServer extends EventEmitter {
    constructor({ port }) {
        super();
        this.init(port);
    }

    init(port) {
        this.responder = zmq.socket('rep');
        this.responder.bind(`tcp://*:${port}`, (err) => {
            if (err) {
                // eslint-disable-next-line no-console
                console.log(err);
            }
            else {
                // eslint-disable-next-line no-console
                console.log(`Listening on ${port}...`);
            }
        });
        this.responder.on('message', async (request) => {
            this.emit('message', request);
        });
        process.on('SIGINT', () => {
            try {
                this.responder.close();
            }
            // eslint-disable-next-line no-empty
            catch (e) {

            }
        });
    }

    send(objToSend) {
        this.responder.send(objToSend);
    }

    close() {
        this.responder.close();
    }
}

module.exports = { Server: ZeroMQServer };
