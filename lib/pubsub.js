var q = require('q'),
    _ = require('lodash'),
    EventEmitter = require('events').EventEmitter;

var subs = {}, id = 0, emitter = new EventEmitter();

emitter.setMaxListeners(0);

function publish(channel, data, cb) {
    emitter.emit(channel, {channel, data});
    emitter.emit('*', {channel, data});
    cb && cb(null);
}

exports.create = function createPubsub() {
    var connId = id++;
    var connSubs = subs[connId] = [];

    return {
        methods: {
            publish,
            subscribe(channel, listener) {
                connSubs.push([channel, listener]);
                emitter.on(channel, listener);
                return () => {
                    emitter.removeListener(channel, listener);
                }
            },
        },
        close() {
            connSubs.forEach(i => emitter.removeListener(i[0], i[1]));
            delete subs[connId];
        }
    }
};

exports.publish = publish;