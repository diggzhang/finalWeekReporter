# _*_ coding: utf8 _*_

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# link to db
dbClient = MongoClient('mongodb://localhost:27017')
db = dbClient['yangcong-prod25']

# open collections
users = db['users']
points = db['points']

startDate = datetime.datetime(2015, 10, 18, 0, 0, 0, 000000)
endDate   = datetime.datetime(2015, 10, 25, 0, 0, 0, 000000)

def playVideoUvWithDateRange(startDate, endDate):
    pipeLine = [
        {"$match": {
            "eventKey": "openVideo",
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            }
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

print("当周视频播放总数：")
print(len(playVideoUvWithDateRange(startDate, endDate)))

def enterHomeUserId(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "eventKey": "enterHome"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allEnterHomeUserId = enterHomeUserId(startDate, endDate)

# processing allEnterHomeUserId as a array
enterHomeUserIdArray = []
for x in allEnterHomeUserId:
    enterHomeUserIdArray.append(x['_id'])
print("进入首页:")
print(len(enterHomeUserIdArray))


# clickSignup

def clickSignupUserId(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "clickSignup"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allClickSignupUserId = clickSignupUserId(startDate, endDate, enterHomeUserIdArray)

clickSignupUserIdArray = []
for x in allClickSignupUserId:
    clickSignupUserIdArray.append(x['_id'])
print("点击注册: ")
print(len(clickSignupUserIdArray))

# enterSignup

def enterSignupUserId(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "enterSignup"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allEnterSignupUserId = enterSignupUserId(startDate, endDate, clickSignupUserIdArray)

enterSignupUserIdArray = []
for x in allEnterSignupUserId:
    enterSignupUserIdArray.append(x['_id'])
print("进入注册页: ")
print(len(enterSignupUserIdArray))

# clickSubmitSignup

def clickSubmitSignup(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "clickSubmitSignup"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allClickSubmitSignupUserId = clickSubmitSignup(startDate, endDate, enterSignupUserIdArray)

clickSubmitSignupUserIdArray = []
for x in allClickSubmitSignupUserId:
    clickSubmitSignupUserIdArray.append(x['_id'])
print("点击提交注册: ")
print(len(clickSubmitSignupUserIdArray))

# tempSignUp

def tempSignUp(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "backendSucceed"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allTempSignUpUserId = tempSignUp(startDate, endDate, clickSubmitSignupUserIdArray)

tempSignUpArray = []
for x in allTempSignUpUserId:
    tempSignUpArray.append(x['_id'])
print("backendSucceed: ")
print(len(tempSignUpArray))

# # tempSignUpGetMe
# def tempSignUpGetMe(startDate, endDate, userId):
#     pipeLine = [
#         {"$match": {
#             "createdBy": {
#                 "$gte": startDate,
#                 "$lt": endDate
#             },
#             "user": {"$in": userId},
#             "eventKey": "tempSignUpGetMe"
#         }},
#         {"$group": {
#             "_id": "$user"
#         }}
#     ]
#     return list(points.aggregate(pipeLine))
# alltempSignUpGetMeUserId = tempSignUpGetMe(startDate, endDate, tempSignUpArray)
#
# tempSignUpGetMeArray = []
# for x in alltempSignUpGetMeUserId:
#     tempSignUpGetMeArray.append(x['_id'])
# print("注册成功: ")
# print(len(tempSignUpGetMeArray))

# openVideo

def openVideo(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "openVideo"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allopenVideoUserId = openVideo(startDate, endDate, tempSignUpArray)

openVideoArray = []
for x in allopenVideoUserId:
    openVideoArray.append(x['_id'])
print("打开视频: ")
print(len(openVideoArray))

# finishVideo

def finishVideo(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "finishVideo"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allfinishVideoUserId = finishVideo(startDate, endDate, openVideoArray)

finishVideoArray = []
for x in allfinishVideoUserId:
    finishVideoArray.append(x['_id'])
print("关闭视频: ")
print(len(finishVideoArray))
