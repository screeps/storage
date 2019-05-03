var q = require('q'),
    _ = require('lodash'),
    net = require('net'),
    EventEmitter = require('events').EventEmitter,
    common = require('@screeps/common'),
    { RpcServer } = common.rpc;
    config = Object.assign(common.configManager.config, {storage: new EventEmitter()}),
    databaseMethods = require('./db'),
    pubsub = require('./pubsub'),
    queueMethods = require('./queue');

Object.assign(config.storage, {
    socketListener(socket) {
        var connectionDesc = `${socket.remoteAddress}:${socket.remotePort}`;

        console.log(`[${connectionDesc}] Incoming connection`);

        socket.on('error', error => console.log(`[${connectionDesc}] Connection error: ${error.message}`));

        var pubsubConnection = pubsub.create();

        new RpcServer(socket, _.extend({}, databaseMethods, queueMethods, pubsubConnection.methods));

        socket.on('close', () => {
            pubsubConnection.close();
            console.log(`[${connectionDesc}] Connection closed`);
        });
    }
});

module.exports.start = function() {

    if (!process.env.STORAGE_PORT) {
        throw new Error('STORAGE_PORT environment variable is not set!');
    }
    if (!process.env.DB_PATH) {
        throw new Error('DB_PATH environment variable is not set!');
    }

    common.configManager.load();

    config.storage.loadDb().then(() => {

        console.log(`Starting storage server`);

        var server = net.createServer(config.storage.socketListener);

        server.on('listening', () => {
            console.log('Storage listening on', process.env.STORAGE_PORT);
            if(process.send) {
                process.send('storageLaunched');
            }
        });

        server.listen(process.env.STORAGE_PORT, process.env.STORAGE_HOST || 'localhost');
    })
    .catch(error => console.error(error));
};