const zmq = require('zeromq');
const EventEmitter = require('events');

class ZeroMQServer extends EventEmitter {
    listen(port) {
        return new Promise((resolve, reject) => {
            this._responder = zmq.socket('rep');
            this._responder.bind(`tcp://*:${port}`, (err) => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
            this._responder.on('message', (request) => {
                this.emit('message', request);
            });
        });
    }

    send(objToSend) {
        // if (Buffer.isBuffer(objToSend)) {
        //     const buffLength = objToSend.length;
        //     const chunks = [];
        //     let i = 0;
        //     while (i < buffLength) {
        //         chunks.push(objToSend.slice(i, i += (1024 * 1024)));
        //     }
        //     this._responder.send(chunks);
        //     return;
        // }
        this._responder.send(objToSend);
    }

    close() {
        this._responder.close();
    }
}

module.exports = { Server: ZeroMQServer };
