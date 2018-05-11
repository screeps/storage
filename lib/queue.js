var q = require('q'),
_ = require('lodash'),
pubsub = require('./pubsub'),
EventEmitter = require('events').EventEmitter;

var queues = {
    usersLegacy: {
        pending: [],
        processing: [],
        emitter: new EventEmitter()
    },
    usersIvm: {
        pending: [],
        processing: [],
        emitter: new EventEmitter()
    },
    rooms: {
        pending: [],
        processing: [],
        emitter: new EventEmitter()
    }
};

module.exports = {
    queueFetch(name, cb) {
        try {
            var check = function () {
                if (!queues[name].pending.length) {
                    queues[name].emitter.once('add', check);
                    return;
                }
                var item = queues[name].pending.pop();
                queues[name].processing.push(item);
                cb(null, item);
            };
            check();
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },
    queueMarkDone(name, id, cb) {
        try {
            _.pull(queues[name].processing, id);
            queues[name].emitter.emit('done');
            cb && cb(null, true);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },
    queueAdd(name, id, cb) {
        try {
            queues[name].pending.push(id);
            queues[name].emitter.emit('add');
            cb && cb(null, true);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },
    queueAddMulti(name, array, cb) {
        try {
            queues[name].pending = queues[name].pending.concat(array);
            queues[name].emitter.emit('add');
            cb && cb(null, true);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },
    queueWhenAllDone(name, cb) {
        try {
            var check = function () {
                if (queues[name].pending.length || queues[name].processing.length) {
                    queues[name].emitter.once('done', check);
                    return;
                }
                pubsub.publish('queueDone:' + name, '1');
                cb(null, true);
            };
            check();
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },
    queueReset(name, cb) {
        try {
            queues[name].pending = [];
            queues[name].processing = [];
            queues[name].emitter.emit('done');
            cb && cb(null, true);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    }
};

setInterval(() => {
    //console.log(queues.users.processing, queues.users.pending);
}, 500);