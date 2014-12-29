var vow = require('vow');
var IdPool = require('./lib/id_pool');
var _ = require('underscore');

var E = {};

var defaultOptions = {
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
    adapter.prototype.init = opts.init;
}

var RealtimeRedisAdapterQueue = createRealtimeAdapterQueue({
    initAgent: function(opts){
        var channel = 'agent_channel_'+this.channelName+'_'+process.pid;
        this.client = require('redis').createClient(opts);
        this.client.on('message', function(ch, message){
            if (channel === ch)
            {
                var info = JSON.parse(message);
                resolveTask(info.data);
                pool.info
            }
        });
        this.client.subscribe('agent_channel_'+this.channelName);
        this.idPool = new IdPool();
        this.pool = {};

    },
    initWorker: function(opts){
        this.client = require('redis').createClient(opts);
        var channel = 'workers_channel_'+this.channelName;
        this.client.on('message', function(ch){
            if (ch==channel)
            {
                accquireTask();
            }
            //var item = pgFreeClient.getClient();
            //logger.log(item);
            //if (item){
            //    redisClient.lpop(LibRecom.job_queue_name, function(err, reply){
            //        if (reply)
            //        {
            //            do_work(reply, item);
            //        }
            //        else
            //        {
            //            item.done();
            //        }
            //    });
            //}
        });
        this.client.subscribe(channel);
    },
    pullTask: function(){
        var dfd = vow.defer();
        var queueName = 'workers_channel_'+this.channelName;
        this.client.lpop(queueName, function(err, reply){
            if (err)
            {
                return void dfd.reject(err);
            }
            if (reply)
            {
                var info = JSON.parse(reply);
                dfd.resolve([info.data, info]);
            }
            else
            {
                dfd.resolve();
            }
        });
        return dfd.promise();
    },
    resolveTask: function(message, info){
        var dfd = vow.defer();
        info.data = message;
        this.client.publish('agent_channel_'+this.channelName+'_'+info.submitter, JSON.parse(info));
        dfd.resolve();
        return dfd.promise();
    },
    pushTask: function(message){
        var self = this;
        var conId = this.idPool.get();
        this.pool[conId.value()] = {
            resource: resource,
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


RealtimeAgent.prototype.pushMessage = function(options){
    var pool = {};
    var opt = _.extend(E.options, options);
    opt.redisClient = opt.redisClient || redis.createClient();
    opt.redisClient.on('message', function(channel, message){
        var info = JSON.parse(message);
        if (pool[info.con_id]){
            var item = pool[info.con_id];
            item.con_id.release();
            if (item.timeout)
                clearTimeout(item.timeout);
            pool[item.con_id.value()] = false;
            opt.successCallback(item.resource, info.data);
        }
    });
    opt.redisClient.subscribe('front'+opt.channelSuffix);
    return E.sendMessage(opt, pool);
};

E.createWorker = function(options){
    var opt = _.extend(E.options, options);
    opt.redisClient = opt.redisClient || redis.createClient();
    return function(){

    };
};


E.sendMessage = function(opt, pool)
{
    return function(resource, message){
        var conId = pool.get();
        pool[conId.value()] = {
            con_id: conId,
            timeout: false
        };
        var info = {
            worker: process.pid + opt.channelSuffix,
            con_id: conId.value(),
            data: message
        };
        opt.redisClient.rpush('workers_queue'+opt.channelSuffix, JSON.stringify(info));
        opt.redisClient.publish('workers_channel'+opt.channelSuffix, 'more');
        pool[conId.value()].timeout = setTimeout(function(){
            if (!pool[conId.value()])
                return;
            if (+reply)
            {
                pool[conId.value()] = false;
                conId.release();
                opt.queueCheckFallback(resource);
            }
            else
            {
                pool[conId.value()].timeout = setTimeout(function(){
                    if (!pool[conId.value()])
                        return;
                    pool[conId.value()] = false;
                    conId.release();
                    opt.timeoutFallback(resource);
                }, opt.delayProcessedCheck);
            }
        }, opt.delayQueueCheck);
    }
};

E.processRequest()

module.exports = E;