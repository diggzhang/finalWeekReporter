// 必须链接到mongo-pri
// 的 mobile_web_tracks
// 无法统计UV
// Robomongo Script

var startDate = ISODate("2015-10-18T00:00:00.000Z");
var endDate   = ISODate("2015-10-25T00:00:00.000Z");

var pvh5 =db.mobile_web_tracks.aggregate([
    {"$match": {
        "localetime": {
            "$gte": startDate,
            "$lte": endDate
        },
        "eventName": "enterMobileSite"
    }}
])
print(pvh5.result.length)
