# import PyPDF2
import re
import json
import copy
import logging
import time
from typing import List, Dict, Any, Optional
from io import BytesIO
from config import config
from .ParserModel import CVParserModel, JDParserModel
from langchain_google_genai import ChatGoogleGenerativeAI
from pymongo import MongoClient, InsertOne, DeleteOne, UpdateOne
from pymongo.collection import Collection


# MongoDB Connection Manager
class MongoDBManager:
    _instance = None
    _client = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = MongoDBManager()
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._client = MongoClient(config.MONGODB_URI)
            
    def get_database(self):
        return self._client[config.MONGODB_DATABASE]
    
    def get_collection(self, collection_name):
        return self.get_database()[collection_name]
    
    def close_connection(self):
        if self._client:
            self._client.close()
            self._client = None
            

# class CVParser():
#     def __init__(self, model):
#         self.model = model
        
#     def extractInformation(self, cv_raw_text):
#         cv_extraction_output = self.model.query(cv_text=cv_raw_text)
#         return cv_extraction_output
    
#     def extractJSONFromText(self, text):
#         # Define JSON pattern
#         json_pattern = r'\{.*\}'
        
#         # Search for JSON string in text
#         match = re.search(json_pattern, text, re.DOTALL)

#         if match:
#             json_str = match.group(0)
#             json_data = json.loads(json_str)
#             return json_data
#         else:
#             return None
    
#     def standardizeCVDict(self, cv_dict, remove_duplicates=True):
#         standardized_cv_dict = copy.deepcopy(cv_dict)
        
#         def dictToText(dict_data):
#             text = '. '.join(f"{key}: {'; '.join(value) if isinstance(value, list) else value}" for key, value in dict_data.items())            
#             return text
                
#         def removeDuplicates(list_data):
#             unique_list = []
#             existed_items = set()

#             for item in list_data:
#                 if item not in existed_items:
#                     unique_list.append(item)
#                     existed_items.add(item)
            
#             return unique_list
        
#         # Standardize each field in the cv dictionary (key (str) and value (str or str list))
#         for key in standardized_cv_dict:
#             if isinstance(standardized_cv_dict[key], list):
#                 for i in range(len(standardized_cv_dict[key])):
#                     if isinstance(standardized_cv_dict[key][i], dict):
#                         standardized_cv_dict[key][i] = dictToText(standardized_cv_dict[key][i])
                        
#                 if remove_duplicates:
#                     standardized_cv_dict[key] = removeDuplicates(list_data=standardized_cv_dict[key])
                    
#             elif isinstance(standardized_cv_dict[key], dict):
#                 standardized_cv_dict[key] = dictToText(standardized_cv_dict[key])
                
#         return standardized_cv_dict
    
#     def parseFromPDF(self, cv_pdf_data, extract_json=True):
#         # Read raw text from PDF and merge multiple pages into a single string
#         pages = []
#         pdf_reader = PyPDF2.PdfReader(BytesIO(cv_pdf_data))

#         for page_num in range(len(pdf_reader.pages)):
#             page = pdf_reader.pages[page_num]
#             pages.append(page.extract_text())

#         cv_raw_text = '\n'.join(pages)

#         # Extract cv info as a string
#         cv_info = self.extractInformation(cv_raw_text=cv_raw_text)
        
#         # Extract cv info as a dict
#         if extract_json:
#             cv_info = self.extractJSONFromText(text=cv_info)
        
#         return cv_info
    
#     @staticmethod
#     def parse_cv(cv_pdf_data):
#       try:
#         parser_gpt_model = CVParserGPT(model_name=config.GPT_MODEL_NAME, token=config.GPT_TOKEN, cv_format=config.CV_JSON_FORMAT)
#         parser = CVParser(model=parser_gpt_model)
#         cv_info_dict = parser.parseFromPDF(cv_pdf_data)
#         cv_info_dict = parser.standardizeCVDict(cv_info_dict)
#         return cv_info_dict
#       except Exception as e:
#         raise Exception("Parsing error:" + str(e))

class JDParser():
    def __init__(self, model):
        self.model = model
        self.mongo_manager = MongoDBManager.get_instance()
        
    def extractInformation(self, jd_text):
        jd_extraction_output = self.model.query(jd_text=jd_text)
        return jd_extraction_output
    
    def batch_extract_information(self, jd_texts_list: List[str]) -> List[str]:
        """Extract information from multiple JDs in batch using LangChain's batch capability"""
        return self.model.batch_query(texts_list=jd_texts_list, batch_size=config.BATCH_SIZE)
    
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
        jd_collection = self.mongo_manager.get_collection(config.MONGODB_JD_COLLECTION)
        parsed_jd_collection = self.mongo_manager.get_collection(config.MONGODB_PARSED_JD_COLLECTION)
        
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
        
        parsed_jd_collection = self.mongo_manager.get_collection(config.MONGODB_PARSED_JD_COLLECTION)
        operations = []
        saved_count = 0
        
        import time
        current_time = time.time()
        
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
                "parsed_data": parsed_jd,
                "parse_timestamp": current_time
            }
            
            # Sử dụng InsertOne thay vì định dạng dictionary
            operations.append(InsertOne(document))
            saved_count += 1
        
        if operations:
            try:
                result = parsed_jd_collection.bulk_write(operations)
                logging.info(f"Đã lưu {result.inserted_count} bản ghi vào MongoDB")
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
                        "parse_timestamp": current_time
                    }
                    
                    try:
                        parsed_jd_collection.insert_one(document)
                        success_count += 1
                    except Exception as inner_e:
                        logging.error(f"Lỗi khi lưu bản ghi {i}: {str(inner_e)}")
                        
                return success_count
        
        return 0
    
    def process_batch_from_mongodb(self, batch_size=None):
        """Fetch, parse, and save a batch of JDs from MongoDB with improved efficiency"""
        if batch_size is None:
            batch_size = config.BATCH_SIZE
        
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
    def parse_jd(jd_data):
      try:
        parser_gemini_model = JDParserModel(
            model_call=ChatGoogleGenerativeAI,
            model_name=config.GEMINI_MODEL_NAME,
            token=config.GEMINI_TOKEN,
            parse_format=config.JD_JSON_FORMAT
        )
        parser = JDParser(model=parser_gemini_model)
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
    def batch_parse_jds_from_mongodb(batch_size=None):
        """Parse multiple JDs from MongoDB using LangChain's batch capabilities"""
        try:
            parser_gemini_model = JDParserModel(
                model_call=ChatGoogleGenerativeAI,
                model_name=config.GEMINI_MODEL_NAME,
                token=config.GEMINI_TOKEN,
                parse_format=config.JD_JSON_FORMAT
            )
            parser = JDParser(model=parser_gemini_model)
            return parser.process_batch_from_mongodb(batch_size)
        except Exception as e:
            raise Exception(f"Batch JD parsing error: {str(e)}")