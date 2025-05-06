from pymongo import MongoClient, InsertOne, DeleteOne, UpdateOne
from pymongo.collection import Collection

# MongoDB Connection Manager
class MongoDBManager:
    _instance = None
    _client = None
    @classmethod
    def get_instance(cls, config=None):
        if cls._instance is None:
            cls._instance = MongoDBManager(config)
        elif config is not None:
            # Update config if provided and instance exists
            cls._instance._config = config
            # Reconnect with new config if client already exists
            if cls._instance._client is not None:
                cls._instance._client.close()
                cls._instance._client = MongoClient(config["MONGODB_URI"])
        return cls._instance
    
    def __init__(self, config=None):
        if config is None:
            raise ValueError("Configuration must be provided")
        self._config = config
        if MongoDBManager._client is None:
            MongoDBManager._client = MongoClient(self._config["MONGODB_URI"])
            
    def get_database(self):
        return self._client[self._config["MONGODB_DATABASE"]]
    
    def get_collection(self, collection_name):
        return self.get_database()[collection_name]
    
    def close_connection(self):
        if self._client:
            self._client.close()
            self._client = None