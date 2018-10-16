/**
 * Created by megan on 2016-11-07.
 */

process.env.NODE_PATH = __dirname;
require('module').Module._initPaths();

var redis = require('./modules/redisModule');
var consistent = require('./modules/consistentModule');
var concurrentHandler = require('handlers/concurrentHandler');

// 필요 모듈 로딩 완료 후 데몬 init
consistent.init([], function(){
    redis.init(function(){
        concurrentHandler.init();
    });
});


