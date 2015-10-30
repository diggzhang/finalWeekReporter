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


print("######### Week Report Part.1 注册 #########")
# 用户共计注册
# db.users.find({}).count()
def totalRegUsers():
    pipeLine = [
        {"$project": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))
print("注册用户共计:")
totalRegUser = len(totalRegUsers())
print(totalRegUser)

# iOS共计注册
# db.users.find({"usefulData.from":"ios"}).count()
def calIosReg():
    pipeLine = [
        {"$match": {
            "usefulData.from": "ios"
        }},
        {"$project": {
            "_id":"$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))
print("iOS注册用户:")
iosRegUserNum = len(calIosReg())
print(iosRegUserNum)

# Android共计注册
# db.users.find({"usefulData.from":"android"}).count()
def calAndroidReg():
    pipeLine = [
        {"$match": {
            "usefulData.from": "android"
        }},
        {"$project": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))
print("Android注册用户:")
androidUserNum = len(calAndroidReg())
print(androidUserNum)

# 用户累计新增人数 首次在洋葱数学网站内产生事件的用户数量
def calNewUserIn():
    pipeLine = [
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

print("累计活动用户数：")
print(len(calNewUserIn()))



#find deterange's all teacher id
def calTeachersId(startDate, endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lte": endDate
            },
            "role": "teacher",
            "rooms": {"$exists": True, "$not": {"$size": 0}}
        }},
        {"$project": {
            "_id":"$_id",
            "rooms": "$rooms"
        }}
    ]
    return list(users.aggregate(pipeLine))

teacherIdAndRoomId = calTeachersId(startDate, endDate)

# stroe all room id
roomsId = []
for room in teacherIdAndRoomId:
    roomsId.extend(room['rooms'])
print("共创建教室:")
print(len(roomsId))

#let all rooms id into room collections get all users

def allStuInRooms(roomsId):
    pipeLine = [
        {"$match": {
            "_id": {"$in": roomsId},
            "users": {"$exists": True, "$not": {"$size": 0}}
        }},
        {"$project": {
            "_id":"$users"
        }}
    ]
    return list(rooms.aggregate(pipeLine))

allStudents = allStuInRooms(roomsId)
# store all studentd id
flatAllstudents = []
for student in allStudents:
    flatAllstudents.extend(student['_id'])

print("教师创建批量用户数:")
teacherCreateStudentsNum = len(flatAllstudents)
print(teacherCreateStudentsNum)

# check flatAllstudents isin points

def stuInRoomAct(startDate, flatAllstudents):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate
            },
            "user": {"$in": flatAllstudents}
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

print("其中激活学生:")
teacherCreateStudentsNumActive = len(stuInRoomAct(startDate, flatAllstudents))
print(teacherCreateStudentsNumActive)

print("PC端 累计活跃用户：")
teacherCreateStudentsNumNotActive = (teacherCreateStudentsNum - teacherCreateStudentsNumActive)
mobileUserNum = iosRegUserNum + androidUserNum
pcActUsersNum = totalRegUser - teacherCreateStudentsNumNotActive - mobileUserNum
print(pcActUsersNum)

print("######### Week Report Part.1 注册 #########")
