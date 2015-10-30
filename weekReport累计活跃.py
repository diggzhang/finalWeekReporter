# _*_ coding: utf8 _*_

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# link to db
dbClient = MongoClient('mongodb://localhost:27017')
db = dbClient['yangcong-prod25']

# open collections
points = db['points']
users = db['users']
rooms = db['rooms']

# configure daterange
startDate = datetime.datetime(2011, 10, 25, 0, 0, 0, 000000)
endDate   = datetime.datetime(2015, 10, 31, 0, 0, 0, 000000)

# 用户累计新增人数 首次在洋葱数学网站内产生事件的用户数量
def calNewUserIn():
    pipeLine = [
        {"$match": {
            "from":"pc"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

newUsersList = calNewUserIn()
newUsersArray = []
for x in newUsersList:
    newUsersArray.append(x['_id'])
print("PC 累计活动用户数：")
print(len(newUsersArray))


def calRegUsers():
    pipeLine = [
        {"$match": {
            "usefulData.from": "signup"
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

regUsersList = calRegUsers()

regUsersArray = []
for x in regUsersList:
    regUsersArray.append(x['_id'])
print("累计创建用户数:")
print(len(regUsersArray))


regActUsers = []
regActUsers = list(set.intersection(set(newUsersArray), set(regUsersArray)))
print("PC端 累计活跃用户数:")
print(len(regActUsers))
