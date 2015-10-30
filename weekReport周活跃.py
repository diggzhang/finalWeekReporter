# _*_ coding: utf8 _*_

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# link to db
dbClient = MongoClient('mongodb://localhost:27017')
db = dbClient['yangcong-prod25']

users = db['users']
points = db['points']

startDate = datetime.datetime(2015, 10, 23, 0, 0, 0, 000000)
endDate   = datetime.datetime(2015, 10, 25, 0, 0, 0, 000000)

def calPcWork(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "from": "pc"
        }},
        {
            "$group": {
                "_id": "$user"
            }
        }
    ]
    return list(points.aggregate(pipeLine))

pcActUsers = calPcWork(startDate, endDate)
pcActUsersArray = []
for x in pcActUsers:
    pcActUsersArray.append(x['_id'])
# print("PC本周活动用户:")
# print(len(pcActUsersArray))

def isUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lt": endDate
            },
            "usefulData.from": "signup"
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

regUsers = isUsers(startDate, endDate)
regUsersArray = []
for x in regUsers:
    regUsersArray.append(x['_id'])
print("PC本周注册用户")
print(len(regUsersArray))

pcOutArray = []
pcOutArray = list(set.intersection(set(regUsersArray), set(pcActUsersArray)))
print("PC周活跃统计")
print(len(pcOutArray))


def calIosWork(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "from": "ios",
        }},
        {
            "$group": {
                "_id": "$user"
            }
        }
    ]
    return list(points.aggregate(pipeLine))

def calAndroidWork(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "from": "android"
        }},
        {
            "$group": {
                "_id": "$user"
            }
        }
    ]
    return list(points.aggregate(pipeLine))


mobileOutList = []
mobileOutList.extend(calIosWork(startDate, endDate));
mobileOutList.extend(calAndroidWork(startDate, endDate));
# print(len(mobileOutList))

mobileOutArray = []
for x in mobileOutList:
    mobileOutArray.append(x['_id'])
print("Mobile 周活跃总计")
print(len(mobileOutArray))

bothOutArray = []
bothOutArray = list(set.intersection(set(pcOutArray), set(mobileOutArray)))
print("两端同活跃统计")
print(len(bothOutArray))
