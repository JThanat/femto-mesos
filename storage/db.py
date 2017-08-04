from pymongo import MongoClient


class MongoInitializer(object):
    HOST = 'localhost'
    PORT = 27917

    def __init__(self, host=HOST, port=PORT, database_name="result_database", collectioname="results"):
        self.mongo_client = MongoClient(host, port)
        self.db = self.mongo_client[database_name]
        self.collection = self.db[collectioname]

    def get_from_key(self, dataset, groupid):
        mongo_object = self.collection.find_one({"dataset": dataset,
                                                 "groupid": groupid
                                                 })
        return mongo_object
