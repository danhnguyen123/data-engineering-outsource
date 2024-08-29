from pymongo import MongoClient
from logging import Logger
from config import config

class MongoDBHeler:

    client = None

    def __init__(self, logger: Logger):

        self.logger = logger

        if self.client is None:
            conn_string = config.MONGODB_CONN
            self.client = MongoClient(conn_string)

    def find(self, database=None, collection=None, *args, **kwargs):
        try:
            db = self.client[database]
            collection = db[collection]
            results = collection.find(*args, **kwargs)
            return list(results)
        except Exception as e:
            self.logger.error(e)
            raise e

    def insert_one(self, database=None, collection=None, document=None):
        try:
            db = self.client[database]
            collection = db[collection]
            collection.insert_one(document=document)
        except Exception as e:
            self.logger.error(e)
            raise e
        
    def insert_many(self, database=None, collection=None, list_document=None):
        try:
            db = self.client[database]
            collection = db[collection]
            collection.insert_many(documents=list_document)
        except Exception as e:
            self.logger.error(e)
            raise e
    
    def update_one(self, database=None, collection=None, contition=None, update_query=None, upsert=False):
        try:
            db = self.client[database]
            collection = db[collection]
            collection.update_one(filter=contition, update=update_query, upsert=upsert)
        except Exception as e:
            self.logger.error(e)
            raise e
        
    def update_many(self, database=None, collection=None, contition=None, update_query=None, upsert=False):
        try:
            db = self.client[database]
            collection = db[collection]
            collection.update_many(filter=contition, update=update_query, upsert=upsert)
        except Exception as e:
            self.logger.error(e)
            raise e

    def truncate_collection(self, database=None, collection=None):
        try:
            db = self.client[database]
            collection = db[collection]
            collection.drop()
        except Exception as e:
            self.logger.error(e)
            raise e


