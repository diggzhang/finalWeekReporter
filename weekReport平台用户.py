# _*_ coding: utf8 _*_

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# link to db
dbClient = MongoClient('mongodb://localhost:27017')
db = dbClient['yangcong-prod25']

# open collections
users = db['users']

startDate = datetime.datetime(2015, 10, 18, 0, 0, 0, 000000)
endDate   = datetime.datetime(2015, 10, 25, 0, 0, 0, 000000)

def calQqPlatformUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                    "$gte": startDate,
                    "$lte": endDate
            },
            "usefulData.q": "qqPlatform"
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

print('QQ平台用户:')
print(len(calQqPlatformUsers(startDate, endDate)))

def calOtherPlatformUsers(startDate,endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lte": endDate
            },
            "usefulData.q": {
                "$exists": True,
                "$ne":"qqPlatform"
            }
        }}
    ]
    return list(users.aggregate(pipeLine))

print('其它平台用户:')
print(len(calOtherPlatformUsers(startDate, endDate)))
