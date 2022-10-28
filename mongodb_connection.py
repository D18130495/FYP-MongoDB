from pymongo import MongoClient


def connect_mongo(database, collection):
    client = MongoClient("mongodb+srv://yushun:qpuur990415@fyp.w0ryzdy.mongodb.net/?retryWrites=true&w=majority")

    db = client[database]
    col = db[collection]
    return col
