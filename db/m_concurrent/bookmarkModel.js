/**
 * Created by megan on 2016-11-14.
 */

module.exports = {
    name: 'bookmark',
    setParameter: function(data){
        return {
            "apDate": data.apDate,   // Bookmark 수신시간
            "logDate": data.logDate,
            "logType": data.logType,
            "deviceType": data.deviceType,
            "itemType": data.itemType,
            "channelType": data.channelType,
            "userNo": data.userNo,
            "programId": data.programId,
            "contentId": data.contentId,
            "cornerId": data.cornerId,
            "mediaTime": data.mediaTime,
            "ipAddress": data.ipAddress,
            "concurrencyGroup": data.concurrencyGroup,               // 0이면 동시시청 제외대상, 1이상은 동시시청 그룹,lives,persmission에서 전달되는 값을 그대로 사용
            "guid": data.guid,
            "playId": data.playId,
            "issue": data.issue
        }
    }
};