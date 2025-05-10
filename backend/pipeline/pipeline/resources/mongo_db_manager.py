from pymongo import MongoClient, InsertOne, DeleteOne, UpdateOne
from pymongo.collection import Collection
import polars as pl
from typing import List, Dict, Any, Optional

# MongoDB Connection Manager
class MongoDBManager:
    _instance = None
    
    @classmethod
    def get_instance(cls, config=None):
        """
        Singleton pattern: Returns the single instance or creates it if it doesn't exist
        Also handles updating configuration and reconnecting if needed
        """
        if cls._instance is None:
            cls._instance = MongoDBManager(config)
        elif config is not None:
            # Update config if provided and instance exists
            cls._instance._config = config
            # Reconnect with new config if client already exists
            if cls._instance._client is not None:
                cls._instance._client.close()
                cls._instance._client = MongoClient(config["MONGODB_URI"])
        
        # Ensure connection is active (auto-reconnect if needed)
        if cls._instance._client is None:
            cls._instance._client = MongoClient(cls._instance._config["MONGODB_URI"])
            
        return cls._instance
    
    def __init__(self, config=None):
        """Initialize the MongoDB manager with configuration"""
        if config is None:
            raise ValueError("Configuration must be provided")
        self._config = config
        self._client = MongoClient(self._config["MONGODB_URI"])
            
    def get_database(self):
        """Get database handle, reconnecting if necessary"""
        # Ensure connection is active before accessing database
        if self._client is None:
            self._client = MongoClient(self._config["MONGODB_URI"])
        return self._client[self._config["MONGODB_DATABASE"]]
    
    def get_collection(self, collection_name):
        """Get collection handle, reconnecting if necessary"""
        return self.get_database()[collection_name]
    
    def close_connection(self):
        """Close the MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            
    def extract_data(self, collection_name: str, query: Dict = None, projection: Dict = None, limit: int = None) -> pl.DataFrame:
        """
        Execute a MongoDB query and return the results as a Polars DataFrame
        
        Args:
            collection_name: Name of the MongoDB collection to query
            query: MongoDB query filter (default: {})
            projection: Fields to include/exclude (default: None, which includes all fields)
            limit: Maximum number of documents to return (default: None, which returns all documents)
            
        Returns:
            Polars DataFrame containing the query results
        """
        # Get collection (this also ensures connection is active)
        collection = self.get_collection(collection_name)
        
        if query is None:
            query = {}
            
        cursor = collection.find(query, projection)
        
        if limit:
            cursor = cursor.limit(limit)
            
        documents = list(cursor)
        
        if not documents:
            return pl.DataFrame()
            
        df = pl.DataFrame(documents)
        return df