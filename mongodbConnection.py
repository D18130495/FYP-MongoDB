from pymongo import MongoClient


def connect_mongo(database, collection):
    client = MongoClient("mongodb+srv://yushun:qpuur990415@fyp.w0ryzdy.mongodb.net/?retryWrites=true&w=majority")

    db = client[database]
    col = db[collection]
    return col


def main():
    col = connect_mongo("fyp", "movie")

    mydict = {"name": "RUNOOB", "alexa": "10000", "url": "https://www.runoob.com"}
    x = col.insert_one(mydict)

    print(col)


if __name__ == '__main__':
    main()
