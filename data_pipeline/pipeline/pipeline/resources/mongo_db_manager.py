from pymongo import MongoClient, InsertOne, DeleteOne, UpdateOne
from pymongo.collection import Collection
import polars as pl
from typing import List, Dict, Any, Optional, Generator, Iterator
from bson import ObjectId


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
    
    def _process_mongodb_objectid(self, documents: List[Dict]) -> List[Dict]:
        """
        Process MongoDB ObjectId fields in documents to make them compatible with Parquet
        
        Args:
            documents: List of MongoDB documents
            
        Returns:
            List of documents with ObjectId converted to strings
        """
        processed_documents = []
        
        for doc in documents:
            # Convert ObjectId to string in the document
            if '_id' in doc and doc['_id'] is not None:
                if isinstance(doc['_id'], ObjectId):
                    doc['_id'] = str(doc['_id'])
                elif isinstance(doc['_id'], dict) and '$oid' in doc['_id']:
                    doc['_id'] = doc['_id']['$oid']
                    
            processed_documents.append(doc)
            
        return processed_documents
    
    def stream_data(self, collection_name: str, query: Dict = None, projection: Dict = None, 
                   batch_size: int = 100) -> Generator[pl.DataFrame, None, None]:
        """
        Stream MongoDB data in batches to reduce memory usage
        
        Args:
            collection_name: Name of the MongoDB collection
            query: MongoDB query filter
            projection: Fields to include/exclude
            batch_size: Number of records to fetch in each batch
            
        Yields:
            Polars DataFrame containing a batch of query results
        """
        collection = self.get_collection(collection_name)
        
        if query is None:
            query = {}
            
        # Create cursor with batch_size for efficient processing
        cursor = collection.find(query, projection).batch_size(batch_size)
        
        # Process in batches
        batch = []
        for doc in cursor:
            batch.append(doc)
            if len(batch) >= batch_size:
                # Process ObjectId before converting to DataFrame
                processed_batch = self._process_mongodb_objectid(batch)
                yield pl.DataFrame(processed_batch)
                batch = []
                
        # Don't forget the last batch which might be smaller
        if batch:
            # Process ObjectId before converting to DataFrame
            processed_batch = self._process_mongodb_objectid(batch)
            yield pl.DataFrame(processed_batch)
            
    def extract_data(self, collection_name: str, query: Dict = None, projection: Dict = None, 
                    limit: int = None, batch_size: int = None) -> pl.DataFrame:
        """
        Execute a MongoDB query and return the results as a Polars DataFrame
        
        Args:
            collection_name: Name of the MongoDB collection to query
            query: MongoDB query filter (default: {})
            projection: Fields to include/exclude (default: None, which includes all fields)
            limit: Maximum number of documents to return (default: None, which returns all documents)
            batch_size: Process in batches to reduce memory usage (default: None)
            
        Returns:
            Polars DataFrame containing the query results
        """
        # Get collection (this also ensures connection is active)
        collection = self.get_collection(collection_name)
        
        if query is None:
            query = {}
        
        # If batch_size is specified, use streaming approach
        if batch_size:
            dfs = []
            doc_count = 0
            for batch_df in self.stream_data(collection_name, query, projection, batch_size):
                dfs.append(batch_df)
                doc_count += batch_df.height
                
                # Apply limit if specified
                if limit and doc_count >= limit:
                    # Trim the last batch if needed
                    if doc_count > limit:
                        excess = doc_count - limit
                        dfs[-1] = dfs[-1].slice(0, batch_df.height - excess)
                    break
            
            # Combine all batches
            if not dfs:
                return pl.DataFrame()
            return pl.concat(dfs)
        
        # Otherwise use original approach
        cursor = collection.find(query, projection)
        
        if limit:
            cursor = cursor.limit(limit)
            
        documents = list(cursor)
        
        if not documents:
            return pl.DataFrame()
        
        # Process ObjectId before converting to DataFrame
        processed_documents = self._process_mongodb_objectid(documents)
        df = pl.DataFrame(processed_documents)
        return df
        
    def count_documents(self, collection_name: str, query: Dict = None) -> int:
        """Count documents matching the query"""
        collection = self.get_collection(collection_name)
        
        if query is None:
            query = {}
            
        return collection.count_documents(query)