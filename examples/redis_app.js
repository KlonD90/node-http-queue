/**
 * Created by nikolaymendyaev on 04.01.15.
 */
var http = require('http');
var http_queue = require('./index.js');
var worker = {
    accquireTask: function(task){
        if (this.working == true)
            return;

    },
    working: false
};
var agent = {
    resolveTask: function(){

    }
};
var redisAdapter = http_queue.createRedisRealtimeAdapterQueue();

var agentQueue = redisAdapter('redis');
var workerQueue = redisAdapter('redis');

var server = http.createServer(function(req, res){

});