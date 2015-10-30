# _*_ coding: utf8 _*_

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# link to db
dbClient = MongoClient('mongodb://localhost:27017')
db = dbClient['yangcong-prod25']
# dbClient = MongoClient('mongodb://10.8.8.8:27017')
# db = dbClient['matrix-yangcong-prod25']

# open collections
points = db['points']
users = db['users']

# lastWeekStartDate = datetime.datetime(2015,10,18)
# lastWeekEndDate   = lastWeekStartDate + datetime.timedelta(days = 6)
#
# startDate = datetime.datetime(2015,10,25)
# endDate   = startDate + datetime.timedelta(days = 6)

lastWeekStartDate = datetime.datetime(2015,10,18)
lastWeekEndDate   = lastWeekStartDate + datetime.timedelta(days = 2)

startDate = datetime.datetime(2015,10,25)
endDate   = startDate + datetime.timedelta(days = 2)

# round 1
def usersInLastWeek(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt":  endDate
            },
            "from":"pc"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

usersInLastWeekList = usersInLastWeek(lastWeekStartDate, lastWeekEndDate)

usersInLastWeekArray = []
for x in usersInLastWeekList:
    usersInLastWeekArray.append(x['_id'])

# Mobile行动
def usersInIosAndAndroid(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "from": {"$in": ['ios', 'android']}
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

usersInIosAndAndroidList = usersInIosAndAndroid(lastWeekStartDate, lastWeekEndDate)

usersInIosAndAndroidArray = []
for x in usersInIosAndAndroidList:
    usersInIosAndAndroidArray.append(x['_id'])

# round 2
# user or mobile users in user collection
def isUser(startDate, endDate):
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

def isMobileUser(startDate, endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lt": endDate
            },
            "usefulData.from": {"$in": ['ios', 'android']}
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

isUserList       = isUser(lastWeekStartDate, lastWeekEndDate)
isMobileUserList = isMobileUser(lastWeekStartDate, lastWeekEndDate)

# points活跃用户中的注册用户
isUserArray = []
for x in isUserList:
    isUserArray.append(x['_id'])

isMobileUserArray = []
for x in isMobileUserList:
    isMobileUserArray.append(x['_id'])

# round 3
# 真正的新增用户数量
# realNewUser = list(set(usersInLastWeekArray) - set(isUserArray))
realNewUser = []
realNewUser = list(set.intersection(set(usersInLastWeekArray), set(isUserArray)))
print("PC 上周新增用户：")
print(len(realNewUser))

# realMobileNewUser = list(set(usersInIosAndAndroidArray) - set(isMobileUserArray))
realMobileNewUser = []
realMobileNewUser = list(set.intersection(set(usersInIosAndAndroidArray), set(isMobileUserArray)))
print("Mobile 上周新增用户")
print(len(realMobileNewUser))

# 上周新增，在本周是否产生行为
def isActThisWeek(startDate, endDate, testUser):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": testUser}
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

pcUser = isActThisWeek(startDate, endDate, realNewUser)
mobileUser = isActThisWeek(startDate, endDate, realMobileNewUser)

print("PC 本周留存:")
print(len(pcUser))
print("Mobile 本周留存:")
print(len(mobileUser))

print("=============================")

usersInThisWeekList = usersInLastWeek(startDate, endDate)
mobileUsersInThisWeekList = usersInIosAndAndroid(startDate, endDate)

usersInThisWeekArray = []
for x in usersInThisWeekList:
    usersInThisWeekArray.append(x['_id'])

mobileUsersInThisWeekArray = []
for x in mobileUsersInThisWeekList:
    mobileUsersInThisWeekArray.append(x['_id'])

thisWeekIsUsers         = isUser(startDate, endDate)
thisWeekIsMobileUsers   = isMobileUser(startDate, endDate)

thisWeekIsUsersArray = []
for x in thisWeekIsUsers:
    thisWeekIsUsersArray.append(x['_id'])

thisWeekMobileUsersArray = []
for x in thisWeekIsMobileUsers:
    thisWeekMobileUsersArray.append(x['_id'])

thisWeekNewUser = []
thisWeekNewUser = list(set.intersection(set(usersInThisWeekArray), set(thisWeekIsUsersArray)))
print("PC 本周新增：")
print(len(thisWeekNewUser))

thisWeekNewMobileUser = []
thisWeekNewMobileUser = list(set.intersection(set(thisWeekMobileUsersArray), set(mobileUsersInThisWeekArray)))
print("Mobile 本周新增:")
print(len(thisWeekNewMobileUser))
