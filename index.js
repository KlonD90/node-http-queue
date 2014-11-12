var vow = require('vow');
var IdPool = require('./lib/id_pool');
var redis = require('./redis');
var _ = require('underscore');


var E = {};



E.options = {
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

E.pushMessage = function(options){
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
            resource: resource,
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