var vow = require('vow');
var IdPool = require('./lib/id_pool');
var _ = require('underscore');

var E = {};

var defaultSettings = {
    delayQueueCheck: 500,
    delayProcessedCheck: 1000,
    channelSuffix: '_default',
    queueCheckFallback: function(resource){
        throw 'not implemented queue check fallback';
    },
    timeoutFallback: function(resource){
        throw 'not implemented timeout fallback';
    },
    successCallback: function(resource, data){
        throw 'not implemented success callback';
    },
    workerLoop: function(){

    }
};

function RealtimeQueue(opts){
    this.settings = _.extend(defaultOptions, opts);

}

function RealtimeAgent(){

}

function RealtimeConsumer(){

}


/*
    RealtimeAdapterQueue

    init
    pullTask model
    pushTask model
    listen
 */

function createRealtimeAdapterQueue(opts){
    var adapter = function(channelName){
        this.channelName = channelName;
    };
    adapter.prototype.pullTask = opts.pullTask;
    adapter.prototype.pushTask = opts.pushTask;
    adapter.prototype.listen = opts.listen;
    adapter.prototype.initAgent = opts.initAgent;
    adapter.prototype.initWorker = opts.initWorker;
    return adapter;
}

function QueueAgentAdapter(opts){
    var adapter = function(channelName){
        this.channelName = channelName;
    };
    adapter.prototype.pullTask = opts.pullTask;
    adapter.prototype.resolveTask = opts.resolveTask;
    adapter.prototype.init = opts.init;
    return adapter;
}

function QueueWorkerAdapter(opts){
    var adapter = function(channelName){
        this.channelName = channelName;
    };
    adapter.prototype.pushTask = opts.pushTask;
    adapter.prototype.resolveTask = opts.resolveTask;
    adapter.prototype.init = opts.init;
    return adapter;
}

function QueueRedisAgentAdapter(redisOpts, resolveTask, settings){
    settings = _.extend(defaultSettings, settings);
    var opts = {
        init: function(cb){
            var channel = 'agent_channel_'+this.channelName+'_'+process.pid;
            this.client = require('redis').createClient(redisOpts);
            this.client.on('message', function(ch, message){
                if (channel === ch)
                {
                    var info = JSON.parse(message);
                    var item = this.pool[info.con_id];
                    if (!item || info.timestamp != item.timestamp)
                        return;
                    item.con_id.release();
                    clearTimeout(conId.timeout);
                    resolveTask(item.response, info.data);
                }
            });
            this.client.subscribe('agent_channel_'+this.channelName);
            this.idPool = new IdPool();
            this.pool = {};
            cb();
        },
        pushTask: function(response, message){
            var self = this;
            var conId = this.idPool.get();
            var timestamp = Date.now();
            this.pool[conId.value()] = {
                response: response,
                con_id: conId,
                timeout: false,
                timestamp: timestamp
            };
            var info = {
                submitter: this.channelName + process.pid,
                con_id: conId.value(),
                data: message,
                timestamp: timestamp
            };
            var jsonValue = JSON.stringify(info);
            this.client.rpush('workers_queue_'+this.channelName, jsonValue);
            this.client.publish('workers_channel_'+this.channelName, '1');
            this.pool[conId.value()].timeout = setTimeout(function(){
                if (!this.pool[conId.value()] || this.pool[conId.value()].timestamp != timestamp)
                    return;
                redisClient.lrem('workers_queue_'+this.channelName, 1, jsonValue, function(err, reply) {
                    if (+reply) {
                        self.pool[conId.value()] = false;
                        conId.release();
                        settings.queueCheckFallback(response);
                    }
                    else {
                        self.pool[conId.value()].timeout = setTimeout(function () {
                            if (!self.pool[conId.value()])
                                return;
                            self.pool[conId.value()] = false;
                            conId.release();
                            settings.timeoutFallback(response);
                        }, settings.delayProcessedCheck);
                    }
                });
            }, settings.delayQueueCheck);
        }
    };
    return QueueAgentAdapter(opts);
}

function QueueRedisWorkerAdapter(redisOpts, resolveTask){
    var opts = {
        init: function(cb){
            this.client = require('redis').createClient(opts);
            var channel = 'workers_channel_'+this.channelName;
            this.client.on('message', function(ch){
                if (ch==channel)
                {
                    accquireTask();
                }
            });
            this.client.subscribe(channel);
            cb();
        },
        pullTask: function(cb){
            var queueName = 'workers_channel_'+this.channelName;
            this.client.lpop(queueName, function(err, reply){
                if (err)
                {
                    return void cb(err);
                }
                if (reply)
                {
                    var info = JSON.parse(reply);
                    cb(null, [info.data, info]);
                }
                else
                {
                    cb(null);
                }
            });
        }
    };
    return QueueWorkerAdapter(opts);
}

function RealtimeRedisAdapterQueue(accquireTask, resolveTask){
    return createRealtimeAdapterQueue({
        initAgent: function(opts, cb){
            var channel = 'agent_channel_'+this.channelName+'_'+process.pid;
            this.client = require('redis').createClient(opts);
            this.client.on('message', function(ch, message){
                if (channel === ch)
                {
                    var info = JSON.parse(message);
                    var item = this.pool[info.con_id];
                    if (!item || info.timestamp != item.timestamp)
                        return;
                    item.con_id.release();
                    clearTimeout(conId.timeout);
                    resolveTask(item.response, info.data);
                }
            });
            this.client.subscribe('agent_channel_'+this.channelName);
            this.idPool = new IdPool();
            this.pool = {};
            cb();
        },
        initWorker: function(opts, cb){
            this.client = require('redis').createClient(opts);
            var channel = 'workers_channel_'+this.channelName;
            this.client.on('message', function(ch){
                if (ch==channel)
                {
                    accquireTask();
                }
            });
            this.client.subscribe(channel);
            cb();
        },
        pullTask: function(cb){
            //var dfd = vow.defer();
            var queueName = 'workers_channel_'+this.channelName;
            this.client.lpop(queueName, function(err, reply){
                if (err)
                {
                    return void cb(err);
                }
                if (reply)
                {
                    var info = JSON.parse(reply);
                    cb(null, [info.data, info]);
                }
                else
                {
                    cb(null);
                }
            });
        },
        workerResolveTask: function(message, info, cb){
            info.data = message;
            this.client.publish('agent_channel_'+this.channelName+'_'+info.submitter, JSON.parse(info));
            cb();
        },
        pushTask: function(response, message){
            var self = this;
            var conId = this.idPool.get();
            this.pool[conId.value()] = {
                response: response,
                con_id: conId,
                timeout: false
            };
            var info = {
                submitter: this.channelName + process.pid,
                con_id: conId.value(),
                data: message
            };
            this.client.rpush('workers_queue_'+this.channelName, JSON.stringify(info));
            this.client.publish('workers_channel_'+this.channelName, '1');
            this.pool[conId.value()].timeout = setTimeout(function(){
                if (!this.pool[conId.value()])
                    return;
                if (+reply)
                {
                    self.pool[conId.value()] = false;
                    conId.release();
                    self.queueCheckFallback(resource);
                }
                else
                {
                    self.pool[conId.value()].timeout = setTimeout(function(){
                        if (!pool[conId.value()])
                            return;
                        self.pool[conId.value()] = false;
                        conId.release();
                        self.timeoutFallback(resource);
                    }, opt.delayProcessedCheck);
                }
            }, opt.delayQueueCheck);
        }
    });
}

E.createRedisRealtimeAdapterQueue = RealtimeRedisAdapterQueue;

module.exports = E;