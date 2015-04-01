import pymongo
from pymongo import MongoClient

def get_mongo_collection( db_name, collection_name):
    mongo_collection = None
    try:
        client = MongoClient()
        mongo_database = client[db_name]
        mongo_collection = mongo_database[collection_name]
    except:
        pass
    return mongo_collection
