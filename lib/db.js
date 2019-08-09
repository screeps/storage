var loki = require('lokijs'),
q = require('q'),
fs = require('fs'),
_ = require('lodash'),
common = require('@screeps/common'),
config = common.configManager.config;

/**
 * @type Loki
 */
var db;

Object.assign(config.storage, {
    dbOptions: {autosave: true, autosaveInterval: 10000},
    getDb() {
        try {
            fs.statSync(process.env.DB_PATH);
        }
        catch (e) {
            fs.writeFileSync(process.env.DB_PATH, '');
        }
        return new loki(process.env.DB_PATH, config.storage.dbOptions);
    },
    loadDb() {
        db = config.storage.getDb();
        return q.ninvoke(db, 'loadDatabase', {})
            .then(upgradeDb);
    }
});

function upgradeDb() {
    let env = db.getCollection('env');
    let envData = env.get(1);
    if(!envData) {
        return;
    }
    if(!envData.databaseVersion || envData.databaseVersion < 3) {
        console.log("Upgrading database to version 3");

    }
    envData.databaseVersion = 3;
    env.update(envData);
}


function updateDocument(doc, update) {
    if (update.$set) {
        _.extend(doc, update.$set);
    }
    if (update.$merge) {
        _.merge(doc, update.$merge);
    }
    if (update.$inc) {
        _.forEach(update.$inc, (val, key) => doc[key] = (doc[key] || 0) + val);
    }
    if (update.$unset) {
        for (var j in update.$unset) {
            delete doc[j];
        }
    }
    if(update.$addToSet) {
        for(let i in update.$addToSet) {
            if(!doc[i]) {
                doc[i] = [];
            }
            if(doc[i].indexOf(update.$addToSet[i]) == -1) {
                doc[i].push(update.$addToSet[i]);
            }
        }
    }
    if(update.$pull) {
        for(let i in update.$pull) {
            if(!doc[i]) {
                continue;
            }
            var idx = doc[i].indexOf(update.$pull[i]);
            if(idx != -1) {
                doc[i].splice(idx, 1);
            }
        }
    }
}

setInterval(function envCleanExpired() {
    var env = db.getCollection('env');
    var values = env.get(1);
    var expiration = env.get(2);
    var dirty = false;
    if (expiration) {
        for (var name in expiration.data) {
            if (Date.now() > expiration.data[name]) {
                dirty = true;
                if (values.data[name]) {
                    delete values.data[name];
                }
                delete expiration.data[name];
            }
        }
    }
    if (dirty) {
        env.update(values);
        env.update(expiration);
    }
}, 10000);

function getRandomString() {
    for (var val = Math.floor(Math.random() * 0x10000).toString(16); val.length < 4; val = '0' + val);
    return val;
}

function genId(obj) {
    var id = getRandomString() + Date.now().toString(16).slice(4) + getRandomString();
    if (obj && !obj._id) {
        obj._id = id;
    }
    return id;
}

function getOrAddCollection(collectionName) {
    var collection = db.getCollection(collectionName);
    if (!collection) {
        collection = db.addCollection(collectionName);
    }
    collection.ensureUniqueIndex('_id');
    switch(collectionName) {
        case 'rooms.objects': {
            collection.ensureIndex('room');
            collection.ensureIndex('user');
            break;
        }
        case 'rooms.intents': {
            collection.ensureIndex('room');
            break;
        }
        case 'users': {
            collection.ensureIndex('username');
            break;
        }
        case 'rooms.flags': {
            collection.ensureIndex('room');
            collection.ensureIndex('user');
            break;
        }
        case 'rooms.terrain': {
            collection.ensureIndex('room');
            break;
        }
        case 'transactions': {
            collection.ensureIndex('user');
            break;
        }
        case 'users.code': {
            collection.ensureIndex('user');
            break;
        }
        case 'users.console': {
            collection.ensureIndex('user');
            break;
        }
        case 'users.money': {
            collection.ensureIndex('user');
            break;
        }
        case 'users.notifications': {
            collection.ensureIndex('user');
            break;
        }
        case 'users.resources': {
            collection.ensureIndex('user');
            break;
        }
        case 'users.power_creeps': {
            collection.ensureIndex('user');
            break;
        }
    }
    return collection;
}

function recursReplaceNeNull(val) {
    if(!_.isObject(val)) {
        return;
    }

    for(var i in val) {
        if (_.isEqual(val[i], {$ne: null}) && !val.$and) {
            val.$and = [{[i]: {$ne: null}}, {[i]: {$ne: undefined}}];
            delete val[i];
        }
        if (_.isEqual(val[i], {$eq: null}) && !val.$or) {
            val.$or = [{[i]: {$eq: null}}, {[i]: {$eq: undefined}}];
            delete val[i];
        }
        recursReplaceNeNull(val[i]);
    }
}

module.exports = {

    dbRequest(collectionName, method, argsArray, cb) {
        try {
            var collection = getOrAddCollection(collectionName);
            if (method == 'insert') {
                if (_.isArray(argsArray[0])) {
                    argsArray[0].forEach(genId);
                }
                else {
                    genId(argsArray[0]);
                }
            }

            if(method == 'find' || method == 'findOne' || method == 'count' || method == 'removeWhere') {
                recursReplaceNeNull(argsArray[0]);
            }

            var result = collection[method].apply(collection, argsArray);
            cb(null, result);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbUpdate(collectionName, query, update, params, cb) {
        try {
            recursReplaceNeNull(query);
            var collection = getOrAddCollection(collectionName);
            var result = [];
            if (Object.keys(query).length == 1 && query._id && _.isString(query._id)) {
                var found = collection.by('_id', query._id);
                if(found) {
                    result = [found];
                }
            }
            else {
                result = collection.find(query);
            }
            if (result.length) {
                result.forEach(doc => {
                    updateDocument(doc, update);
                    collection.update(doc);
                });
                cb(null, {modified: result.length});
            }
            else if (params && params.upsert) {
                var item = {};
                if (query.$and) {
                    query.$and.forEach(i => _.extend(item, i));
                }
                else {
                    _.extend(item, query);
                }
                updateDocument(item, update);
                genId(item);
                collection.insert(item);
                cb(null, {inserted: 1});
            }
            else {
                cb(null, {});
            }
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbBulk(collectionName, bulk, cb) {
        try {
            var collection = getOrAddCollection(collectionName), result;
            bulk.forEach(i => {
                switch (i.op) {
                    case 'update': {
                        result = collection.by('_id', i.id);
                        if (result) {
                            updateDocument(result, i.update);
                            collection.update(result);
                        }
                        break;
                    }
                    case 'insert': {
                        genId(i.data);
                        collection.insert(i.data);
                        break;
                    }
                    case 'remove': {
                        result = collection.by('_id', i.id);
                        if (result) {
                            collection.remove(result);
                        }
                        break;
                    }
                }
            });
            cb(null);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbFindEx(collectionName, query, opts, cb) {
        try {
            recursReplaceNeNull(query);
            var collection = getOrAddCollection(collectionName);
            var chain = collection.chain().find(query);
            if (opts.sort) {
                for (var field in opts.sort) {
                    chain = chain.simplesort(field, opts.sort[field] == -1);
                }
            }
            if (opts.offset) {
                chain = chain.offset(opts.offset);
            }
            if (opts.limit) {
                chain = chain.limit(opts.limit);
            }
            cb(null, chain.data());
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbEnvGet(name, cb) {
        try {
            var item = db.getCollection('env').get(1) || {data: {}};
            cb(null, item.data[name]);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbEnvMget(names, cb) {
        try {
            var item = db.getCollection('env').get(1) || {data: {}};
            var result = names.map(name => item.data[name]);
            cb(null, result);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbEnvSet(name, value, cb) {
        try {
            var env = db.getCollection('env');
            var values = env.get(1);
            if (values) {
                values.data[name] = value;
                env.update(values);
            }
            else {
                values = {data: {[name]: value}};
                env.insert(values);
            }
            cb && cb(null, value);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbEnvExpire(name, seconds, cb) {
        try {
            var env = db.getCollection('env');
            var expiration = env.get(2);
            if (expiration) {
                expiration.data[name] = Date.now() + seconds * 1000;
                env.update(expiration);
            }
            else {
                expiration = {data: {[name]: Date.now() + seconds * 1000}};
                env.insert(expiration);
            }
            cb && cb(null);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbEnvSetex(name, seconds, value, cb) {
        try {
            module.exports.dbEnvSet(name, value);
            module.exports.dbEnvExpire(name, seconds);
            cb(null);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbEnvTtl(name, cb) {
        try {
            var env = db.getCollection('env');
            var expiration = env.get(2);
            if (!expiration || !expiration.data[name] || expiration.data[name] < Date.now()) {
                cb(null, -1);
                return;
            }
            cb(null, (expiration.data[name] - Date.now()) / 1000);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbEnvDel(name, cb) {
        try {
            var env = db.getCollection('env');
            var values = env.get(1);
            if (values && values.data[name]) {
                delete values.data[name];
                cb(null, 1);
            }
            else {
                cb(null, 0);
            }
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbEnvHget(name, field, cb) {
        try {
            var env = db.getCollection('env');
            var values = env.get(1);
            if (values && values.data && values.data[name]) {
                cb(null, values.data[name][field]);
            }
            else {
                cb(null);
            }
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbEnvHset(name, field, value, cb) {
        try {
            var env = db.getCollection('env');
            var values = env.get(1);
            if (values) {
                values.data[name] = values.data[name] || {};
                values.data[name][field] = value;
                env.update(values);
            }
            else {
                values = {data: {[name]: {[field]: value}}};
                env.insert(values);
            }
            cb(null, values.data[name][field]);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbEnvHmget(name, fields, cb) {
        try {
            var env = db.getCollection('env');
            var values = env.get(1) || {data: {}};
            values.data[name] = values.data[name] || {};
            var result = fields.map(i => values.data[name][i]);
            cb(null, result);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbEnvHmset(name, data, cb) {
        try {
            var env = db.getCollection('env');
            var values = env.get(1);
            if (values) {
                values.data[name] = values.data[name] || {};
                _.extend(values.data[name], data);
                env.update(values);
            }
            else {
                values = {data: {[name]: data}};
                env.insert(values);
            }
            cb(null, values.data[name]);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    },

    dbResetAllData(cb) {
        try {
            db.loadJSON(JSON.stringify(require('../db.original')));
            cb(null);
        }
        catch (e) {
            cb(e.message);
            console.error(e);
        }
    }
};
