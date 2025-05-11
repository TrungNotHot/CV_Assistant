import re
import json
import copy
import logging
from typing import List, Dict, Any, Optional
from .ParserModel import JDParserModel
from langchain_google_genai import ChatGoogleGenerativeAI
from .mongo_db_manager import MongoDBManager
from pymongo import MongoClient, InsertOne, DeleteOne, UpdateOne
from datetime import datetime

class JDParser():
    def __init__(self, model, mongo_config=None):
        self.model = model
        self.mongo_config = mongo_config
        self.mongo_manager = MongoDBManager.get_instance(mongo_config)
        
    def extractInformation(self, jd_text):
        jd_extraction_output = self.model.query(jd_text=jd_text)
        return jd_extraction_output
    
    def batch_extract_information(self, jd_texts_list: List[str]) -> List[str]:
        """Extract information from multiple JDs in batch using LangChain's batch capability"""
        return self.model.batch_query(texts_list=jd_texts_list)
    
    def extractJSONFromText(self, text):
        # Define JSON pattern
        json_pattern = r'\{.*\}'
        
        # Search for JSON string in text
        match = re.search(json_pattern, text, re.DOTALL)

        if match:
            json_str = match.group(0)
            try:
                json_data = json.loads(json_str)
                return json_data
            except json.JSONDecodeError:
                logging.error(f"Failed to decode JSON: {json_str}")
                return None
        else:
            return None
    
    def batch_extract_json_from_texts(self, texts_list: List[str]) -> List[Dict]:
        """Extract JSON from multiple model outputs"""
        results = []
        for text in texts_list:
            if text:  # Check if text is not None
                json_data = self.extractJSONFromText(text)
                results.append(json_data)
            else:
                results.append(None)
        return results
    
    def standardizeJDDict(self, jd_dict, remove_duplicates=True):
        if jd_dict is None:
            return None
        
        standardized_jd_dict = copy.deepcopy(jd_dict)
        
        def dictToText(dict_data):
            text = '. '.join(f"{key}: {'; '.join(value) if isinstance(value, list) else value}" for key, value in dict_data.items())            
            return text
                
        def removeDuplicates(list_data):
            unique_list = []
            existed_items = set()

            for item in list_data:
                if item not in existed_items:
                    unique_list.append(item)
                    existed_items.add(item)
            return unique_list
        
        # Standardize each field in the jd dictionary (key (str) and value (str or str list))
        for key in standardized_jd_dict:
            if isinstance(standardized_jd_dict[key], list):
                for i in range(len(standardized_jd_dict[key])):
                    if isinstance(standardized_jd_dict[key][i], dict):
                        standardized_jd_dict[key][i] = dictToText(standardized_jd_dict[key][i])
                        
                if remove_duplicates:
                    standardized_jd_dict[key] = removeDuplicates(list_data=standardized_jd_dict[key])
                    
            elif isinstance(standardized_jd_dict[key], dict):
                standardized_jd_dict[key] = dictToText(standardized_jd_dict[key])
        return standardized_jd_dict
    
    def batch_standardize_jd_dicts(self, jd_dicts_list: List[Dict]) -> List[Dict]:
        """Standardize multiple JD dictionaries"""
        results = []
        for jd_dict in jd_dicts_list:
            standardized_dict = self.standardizeJDDict(jd_dict)
            results.append(standardized_dict)
        return results
    
    def parseFromText(self, jd_text, extract_json=True):
        # Extract jd info as a string
        jd_info = self.extractInformation(jd_text=jd_text)
        
        # Extract jd info as a dict
        if extract_json:
            jd_info = self.extractJSONFromText(text=jd_info)
        return jd_info
    
    def fetch_unparsed_jds_from_mongodb(self, limit=None) -> List[Dict]:
        """Fetch unparsed JDs from MongoDB"""
        jd_collection = self.mongo_manager.get_collection(self.mongo_config["MONGODB_JD_COLLECTION"])
        parsed_jd_collection = self.mongo_manager.get_collection(self.mongo_config["MONGODB_PARSED_JD_COLLECTION"])
        
        # Find all parsed JD IDs
        parsed_ids = [doc['_id'] for doc in parsed_jd_collection.find({}, {'_id': 1})]
        
        # Find JDs that haven't been parsed yet
        query = {'_id': {'$nin': parsed_ids}} if parsed_ids else {}
        
        if limit:
            unparsed_jds = list(jd_collection.find(query).limit(limit))
        else:
            unparsed_jds = list(jd_collection.find(query))
            
        return unparsed_jds
    
    def save_parsed_jds_to_mongodb(self, original_jds: List[Dict], parsed_jds: List[Dict]) -> int:
        """Save parsed JDs to MongoDB using bulk operations for efficiency"""
        if not original_jds or not parsed_jds or len(original_jds) != len(parsed_jds):
            logging.error("Mismatch between original and parsed JDs lists")
            return 0
        
        parsed_jd_collection = self.mongo_manager.get_collection(self.mongo_config["MONGODB_PARSED_JD_COLLECTION"])
        operations = []
        saved_count = 0
        
        current_time = datetime.now().strftime("%Y-%m-%d")
        
        for i, (original_jd, parsed_jd) in enumerate(zip(original_jds, parsed_jds)):
            if parsed_jd is None:
                continue
                
            # Create document with original JD metadata and parsed content
            document = {
                "_id": original_jd["_id"],
                "job_title": original_jd.get("job_title", ""),
                "company_name": original_jd.get("company_name", ""),
                "location": original_jd.get("location", ""),
                "posted_date": original_jd.get("posted_date", ""),
                "url": original_jd.get("url", ""),
                "update_date": current_time
            }
            document.update(parsed_jd)
            
            # Sử dụng InsertOne thay vì định dạng dictionary
            operations.append(InsertOne(document))
            saved_count += 1
        
        if operations:
            try:
                result = parsed_jd_collection.bulk_write(operations)
                logging.info(f"Saved {result.inserted_count} datapoints to MongoDB")
                return saved_count
            except Exception as e:
                logging.error(f"Error in bulk write to MongoDB: {str(e)}")
                # Thử lưu từng bản ghi một nếu bulk_write thất bại
                success_count = 0
                for i, (original_jd, parsed_jd) in enumerate(zip(original_jds, parsed_jds)):
                    if parsed_jd is None:
                        continue
                    
                    document = {
                        "_id": original_jd["_id"],
                        "job_title": original_jd.get("job_title", ""),
                        "company_name": original_jd.get("company_name", ""),
                        "location": original_jd.get("location", ""),
                        "posted_date": original_jd.get("posted_date", ""),
                        "url": original_jd.get("url", ""),
                        "parsed_data": parsed_jd,
                        "update_date": current_time
                    }
                    
                    try:
                        parsed_jd_collection.insert_one(document)
                        success_count += 1
                    except Exception as inner_e:
                        logging.error(f"Lỗi khi lưu bản ghi {i}: {str(inner_e)}")
                        
                return success_count
        
        return 0
    
    def process_batch_from_mongodb(self, batch_size=None, config=None):
        """Fetch, parse, and save a batch of JDs from MongoDB with improved efficiency"""
        if batch_size is None and config and "BATCH_SIZE" in config:
            batch_size = config["BATCH_SIZE"]
        elif batch_size is None:
            batch_size = 10  # Default batch size if not provided
        
        # Fetch unparsed JDs
        unparsed_jds = self.fetch_unparsed_jds_from_mongodb(limit=batch_size)
        
        if not unparsed_jds:
            logging.info("No unparsed JDs found in MongoDB")
            return 0
        
        # Extract JD texts
        jd_texts = [jd.get('full_text_jd', '') for jd in unparsed_jds]
        
        # Use LangChain's batch processing
        parsed_texts = self.batch_extract_information(jd_texts)
        parsed_jsons = self.batch_extract_json_from_texts(parsed_texts)
        standardized_jsons = self.batch_standardize_jd_dicts(parsed_jsons)
        
        # Save results to MongoDB using bulk operations
        saved_count = self.save_parsed_jds_to_mongodb(unparsed_jds, standardized_jsons)
        
        return saved_count
    
    @staticmethod
    def parse_jd(jd_data, mongo_config=None, config=None):
      try:
        parser_gemini_model = JDParserModel(
            model_call=ChatGoogleGenerativeAI,
            model_name=config["GEMINI_MODEL_NAME"] if config else None,
            token=config["GEMINI_TOKEN"] if config else None,
            parse_format=config["JD_JSON_FORMAT"] if config else None
        )

        parser = JDParser(model=parser_gemini_model, mongo_config=mongo_config)

        jd_info_dict = parser.parseFromText(jd_data['full_text_jd'])
        jd_info_dict = parser.standardizeJDDict(jd_info_dict)
        result_data = {
            "job_title": jd_data['job_title'],
            "company_name": jd_data['company_name'],
            "location": jd_data['location'],
            "posted_date": jd_data['posted_date'],
            "url": jd_data['url'],
        }
        result_data.update(jd_info_dict)
        return result_data
      except Exception as e:
        raise Exception("JD parsing error: " + str(e))
    
    @staticmethod
    def batch_parse_jds_from_mongodb(mongo_config=None, batch_size=None, config=None):
        """Parse multiple JDs from MongoDB using LangChain's batch capabilities"""
        try:
            parser_gemini_model = JDParserModel(
                model_call=ChatGoogleGenerativeAI,
                model_name=config["GEMINI_MODEL_NAME"] if config else None,
                token=config["GEMINI_TOKEN"] if config else None,
                parse_format=config["JD_JSON_FORMAT"] if config else None
            )
            parser = JDParser(model=parser_gemini_model, mongo_config=mongo_config)
            return parser.process_batch_from_mongodb(batch_size, config)
        except Exception as e:
            raise Exception(f"Batch JD parsing error: {str(e)}")