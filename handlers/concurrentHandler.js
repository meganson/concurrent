/**
 * Created by megan on 2016-11-23.
 */

var megan = require('modules');

module.exports = {
    init: function () {
        this._logTypeCnt = 0;
        this._groupCnt = 0;
        this._userNoCnt = 0;
        this._playIdCnt = 0;
        this._deviceTypeCnt = 0;
        this._apDateCnt = 0;
        this._contentIdCnt = 0;
        this._issueCnt = 0;
        this._ipAddCnt = 0;
        this._guidCnt = 0;
        this._successTotalCnt = 0;
        this._failTotalCnt = 0;
        this._setexTotalCnt = 0;

        this.start();
    },
    start: function(){
        this.triggerConsumer();
        this.triggerStatistics();
    },
    triggerConsumer: function () {
        var self = this;
        var kafkaConf = megan.config.conf.kafka;

        megan.log.info('trigger Consumer.....');

        var options = {
            host: kafkaConf.connectionString,
            clientId: kafkaConf.clientId,
            // zk : undefined,   // put client zk settings if you need them (see Client)
            // batch: undefined, // put client batch settings if you need them (see Client)
            groupId: kafkaConf.groupIdK,
            sessionTimeout: 15000,

            // An array of partition assignment protocols ordered by preference.
            // 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
            // protocol: ['roundrobin'],

            // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms
            fetchMaxWaitMs: 10,
            // fetchMaxBytes: 1024 * 1024,

            // Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved)
            // equivalent to Java client's auto.offset.reset
            // fromOffset: 'latest', // default

            // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
            outOfRangeOffset: 'latest',

            migrateHLC: false,    // for details please see Migration section below
            migrateRolling: true
        };
        var topics = kafkaConf.topic;
        megan.kafka.consumerGroup(options, topics, function(msg){

        // var payloads = {
        //     topic: kafkaConf.topic
        // };
        // var options = {
        //     groupId: kafkaConf.groupId,
        //     // Auto commit config
        //     autoCommit: true,
        //     // If set true, consumer will fetch message from the given offset in the payloads
        //     fromOffset: 'latest',
        //     // If set to 'buffer', values will be returned as raw buffer objects.
        //     encoding: 'utf8'
        // };
        //
        // megan.kafka.highLevelConsumer(payloads, options, function (msg) {
            //{"logDate":"2016-09-11 16:16:16+0000","logType":"I","deviceType":"2","itemType":"1","channelType":"L","userNo":"2811301","programId":"M_00000000001","contentId":"MC_00000000001","cornerId":"1","mediaTime":"01:20:30","ipAddress":"127.0.0.1","concurrencyGroup":1,"extra":{"errorCode":"30004","osVer":"4.0","appVer":"2.2.4","deviceName":"SHW-M110S","networkType":"1","playerType":"6","isAllow":"Y","guid":"142c74fc-783b-11e6-a219-067498002a25","playId":"01d5a682c30d4d01a7eeacc3318a8226.5"}}
            // try {
                if(megan.common.isJsonString(msg)){
                    var message = JSON.parse(msg);
                    if(!megan.common.isNull(message)){
                        if(megan.common.isNull(message.logType)){
                            self._logTypeCnt++;
                            self._failTotalCnt++;
                            megan.log.debug('no logType');
                            return;
                        }
                        if(message.logType == 'I'){ // I : 시청로그, E : 에러, R : 에러후 리트라이시 ( 동시시청 제어에서는 시청로그(I)만 사용 )
                            if(megan.common.isNull(message.concurrencyGroup)){
                                self._groupCnt++;
                                self._failTotalCnt++;
                                megan.log.debug('no group');
                                return;
                            }
                            if(megan.common.isNull(message.userNo)){
                                self._userNoCnt++;
                                self._failTotalCnt++;
                                megan.log.debug('no userNo');
                                return;
                            }
                            if(megan.common.isNull(message.playId)){
                                self._playIdCnt++;
                                self._failTotalCnt++;
                                megan.log.debug('no playId');
                                return;
                            }
                            if(megan.common.isNull(message.deviceType)){
                                self._deviceTypeCnt++;
                                self._failTotalCnt++;
                                megan.log.debug('no deviceType');
                                return;
                            }
                            if(megan.common.isNull(message.apDate)){
                                self._apDateCnt++;
                                self._failTotalCnt++;
                                megan.log.debug('no apDate');
                                return;
                            }
                            if(megan.common.isNull(message.contentId)){
                                message.contentId = message.programId;
                                self._contentIdCnt++;
                                // megan.log.info(msg);
                                // return;
                            }
                            if(megan.common.isNull(message.issue)){
                                self._issueCnt++;
                                self._failTotalCnt++;
                                megan.log.debug('no issue');
                                return;
                            }
                            if(megan.common.isNull(message.ipAddress)){
                                self._ipAddCnt++;
                                self._failTotalCnt++;
                                megan.log.debug('no ipAddress');
                                return;
                            }
                            if(megan.common.isNull(message.guid)){
                                self._guidCnt++;
                                self._failTotalCnt++;
                                megan.log.debug('no guid');
                                return;
                            }

                            var params = megan.bookmarkModel.setParameter(message);
                            if(params.concurrencyGroup > '0'){
                                var now = megan.date.getDatetime();
                                var apDate = megan.date.getDatetimeFormat(params.apDate);
                                var diffTime = Math.round((now - apDate) / 1000);
                                var ttl = megan.config.conf.sessionTtl - diffTime || 25;
                                if(ttl > 0){
                                    self._successTotalCnt++;
                                    var group = params.concurrencyGroup;
                                    var userNo = params.userNo;
                                    var playId = params.playId;
                                    var key = 'b:'+group+':'+userNo+':'+playId;
                                    // if(!megan.common.isNull(userNo) && !(megan.common.isNull(playId))){
                                        megan.consistent.getNode(userNo, function (namespace) {
                                            // self._setexTotalCnt++;
                                            megan.redis.setex(namespace, key, self.getValue(params), ttl);
                                        });
                                    // }
                                    return;
                                }
                                // megan.log.debug('minus ttl :', ttl);
                            }
                        }
                    }
                    return;
                }
                megan.log.error('no json', msg);
            // }catch(e){
            //     megan.log.error(e, msg);
            // }
        });
    },
    getValue: function(val){
        return JSON.stringify({
            "deviceType": val.deviceType,
            "apDate": val.apDate,
            "contentId": val.contentId,
            "issue": val.issue,
            "ipAddress": val.ipAddress,
            "guid": val.guid
        });
    },
    triggerStatistics: function(){
        var self = this;
        megan.log.info('trigger Statistics.....');

        setInterval(function(){
            megan.log.warn('=============================================');
            megan.log.warn('no logType cnt', self._logTypeCnt);
            megan.log.warn('no group cnt', self._groupCnt);
            megan.log.warn('no userNo cnt', self._userNoCnt);
            megan.log.warn('no playId cnt', self._playIdCnt);
            megan.log.warn('no deviceType cnt', self._deviceTypeCnt);
            megan.log.warn('no apDate cnt', self._apDateCnt);
            megan.log.warn('no contentId cnt', self._contentIdCnt);
            megan.log.warn('no issue cnt', self._issueCnt);
            megan.log.warn('no ipAddress cnt', self._ipAddCnt);
            megan.log.warn('no guid cnt', self._guidCnt);
            megan.log.warn('success cnt', self._successTotalCnt);
            megan.log.warn('setex cnt', self._setexTotalCnt);
            megan.log.warn('fail cnt', self._failTotalCnt);
            megan.log.warn('=============================================');

            self._logTypeCnt = 0;
            self._groupCnt = 0;
            self._userNoCnt = 0;
            self._playIdCnt = 0;
            self._deviceTypeCnt = 0;
            self._apDateCnt = 0;
            self._contentIdCnt = 0;
            self._issueCnt = 0;
            self._ipAddCnt = 0;
            self._guidCnt = 0;
            self._successTotalCnt = 0;
            self._setexTotalCnt = 0;
            self._failTotalCnt = 0;
        }, (megan.config.conf.statisticsMinutes || 10) * 60 * 1000)
    }
};