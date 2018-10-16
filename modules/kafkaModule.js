/**
 * Created by megan on 2016-11-07.
 */

var kafka = require('kafka-node');
var Consumer = kafka.Consumer;
var HighLevelConsumer = kafka.HighLevelConsumer;
var Producer = kafka.Producer;
var ConsumerGroup = kafka.ConsumerGroup;
var config = require('./configModule');
var log = require('./logModule');

var KafkaModule = function(){
    var connectionString = config.conf.kafka.connectionString;
    this._client = new kafka.Client(connectionString);
};

KafkaModule.prototype.producer = function(payloads, options){
    var producer = new Producer(this._client, options);

    producer.on('ready', function () {
        producer.send(payloads, function (err, data) {
            log.info(data);
        });
    });
};

KafkaModule.prototype.highLevelProducer = function(payloads){
    var HighLevelProducer = kafka.HighLevelProducer,
        producer = new HighLevelProducer(this._client);

    producer.on('ready', function () {
        producer.send(payloads, function (err, data) {
            log.info(data);
        });
    });
};

KafkaModule.prototype.consumer = function(payloads, options, callback){
    var consumer = new Consumer(
        this._client,
        [
            payloads
        ],
        options
    );

    consumer.on('message', function (message) {
        log.info(message);
        callback(message);
    });

    consumer.on('error', function (err) {
        log.error(err);
    });
};

KafkaModule.prototype.highLevelConsumer = function(payloads, options, callback){
    var consumer = new HighLevelConsumer(
        this._client,
        [
            payloads
        ],
        options
    );

    consumer.on('message', function (message) {
        // debugging 을 위한 msg 무조건 저장
        // log.info(message);
        callback(message.value);
    });

    consumer.on('error', function (e) {
        log.error('kafka consumer error', e);
    });
};

KafkaModule.prototype.consumerGroup = function(options, topics, callback){
    var consumerGroup = new ConsumerGroup(options, topics);

    consumerGroup.on('message', function(message){
        // debugging 을 위한 msg 무조건 저장
        // log.info(message);
        callback(message.value);
    });

    consumerGroup.on('error', function (e) {
        log.error('kafka consumerGroup error', e);
    });

    consumerGroup.on('offsetOutOfRange', function(e){
        log.error('kafka consumerGroup out of Range', e);
    });
};

module.exports = new KafkaModule();