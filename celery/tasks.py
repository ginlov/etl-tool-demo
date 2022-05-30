# import pymongo
from celery import Celery

app = Celery('tasks', backend='redis://localhost', broker='redis://localhost:6379/0')

@app.task()
def add(x, y):
    return x + y

@app.task()
def extract_mongo():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["mydatabase"]
    mycol = mydb["customers"]
    for x in mycol.find():
        print(x)

#docker run -d -p 5672:5672 rabbitmq
#docker run -d -p 6379:6379 redis
#celery -A tasks worker --loglevel=INFO