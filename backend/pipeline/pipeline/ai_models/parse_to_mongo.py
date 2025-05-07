import json
import logging
import os
from dotenv import load_dotenv
from Parser import JDParser
from ParserModel import JDParserModel
from mongo_db_manager import MongoDBManager
from langchain_google_genai import ChatGoogleGenerativeAI

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), "../../../.env"))

# Create MongoDB configuration dictionary
config = {
    "GEMINI_MODEL_NAME": os.getenv("GEMINI_MODEL_NAME"),
    "GEMINI_TOKEN": os.getenv("GEMINI_TOKEN"),
    "JD_JSON_FORMAT": str(os.getenv("JD_JSON_FORMAT")),
    "BATCH_SIZE": int(os.getenv("BATCH_SIZE")),
}
mongo_config = {
    "MONGODB_URI": os.getenv("MONGODB_URI"),
    "MONGODB_DATABASE": os.getenv("MONGODB_DATABASE"),
    "MONGODB_JD_COLLECTION": os.getenv("MONGODB_JD_COLLECTION"),
    "MONGODB_PARSED_JD_COLLECTION": os.getenv("MONGODB_PARSED_JD_COLLECTION")
}
# Configure logging to view detailed information
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Test data path
test_data_path = os.path.join("/home/trungnothot/Study/CV_Assistant/backend/test/job_descriptions_limited.json")



# Check MongoDB connection
try:
    mongo_manager = MongoDBManager.get_instance(mongo_config)
    db = mongo_manager.get_database()
    logging.info(f"MongoDB connection successful. Database: {mongo_config['MONGODB_DATABASE']}")
except Exception as e:
    logging.error(f"MongoDB connection error: {str(e)}")

# Check document count in collection
try:
    jd_collection = mongo_manager.get_collection(mongo_config["MONGODB_JD_COLLECTION"])
    count = jd_collection.count_documents({})
    logging.info(f"Number of JDs in collection '{mongo_config['MONGODB_JD_COLLECTION']}': {count}")
    
    if count == 0:
        logging.info("No data in collection. Trying to add sample data.")
        
        # Read and add sample data to MongoDB if no data exists
        if os.path.exists(test_data_path):
            with open(test_data_path, 'r') as f:
                test_data = json.load(f)
                
            # Keep the existing _id if available or generate a new one
            for i, item in enumerate(test_data):
                if '_id' not in item:
                    # Generate a new ID if none exists
                    item['_id'] = f"test_jd_{i}"
                else:
                    # Ensure _id is in the correct format if it already exists
                    item['_id'] = str(item['_id']["$oid"])
            
            # Add to MongoDB
            try:
                result = jd_collection.insert_many(test_data)
                logging.info(f"Added {len(result.inserted_ids)} sample records to MongoDB")
            except Exception as e:
                logging.error(f"Error adding sample data: {str(e)}")
except Exception as e:
    logging.error(f"Error checking collection: {str(e)}")

# Process all JDs from MongoDB in batches
try:
    # Get count of unprocessed JDs
    parser_gemini_model = JDParserModel(
        model_call=ChatGoogleGenerativeAI,
        model_name=config["GEMINI_MODEL_NAME"],
        token=config["GEMINI_TOKEN"],
        parse_format=config["JD_JSON_FORMAT"]
    )
    parser = JDParser(model=parser_gemini_model, mongo_config=mongo_config)
    
    # Get list of unparsed JDs
    unparsed_jds = parser.fetch_unparsed_jds_from_mongodb()
    total_jds = len(unparsed_jds)
    
    if total_jds == 0:
        logging.info("No JDs need processing")
    else:
        batch_size = config["BATCH_SIZE"]
        logging.info(f"Found {total_jds} JDs to process, with batch_size = {batch_size}")
        
        # Calculate number of batches
        # num_batches = (total_jds + batch_size - 1) // batch_size  # Round up
        num_batches=1
        
        # Process by batch
        total_processed = 0
        for batch_num in range(num_batches):
            logging.info(f"Processing batch {batch_num + 1}/{num_batches}...")
            try:
                processed_count = JDParser.batch_parse_jds_from_mongodb(mongo_config=mongo_config, batch_size=batch_size, config=config)
                total_processed += processed_count
                logging.info(f"Processed {processed_count} JDs in batch {batch_num + 1}. Total processed: {total_processed}/{total_jds}")
            except Exception as batch_e:
                logging.error(f"Error processing batch {batch_num + 1}: {str(batch_e)}")
        
        logging.info(f"Completed! Processed a total of {total_processed} JDs")
        
except Exception as e:
    logging.error(f"Error processing JDs: {str(e)}")
    
    # If processing from MongoDB fails, try processing sample data directly
    try:
        logging.info("Trying to process sample data directly...")
        if os.path.exists(test_data_path):
            with open(test_data_path, 'r') as f:
                test_data = json.load(f)
            
            if test_data:
                parser_gemini_model = JDParserModel(
                    model_call=ChatGoogleGenerativeAI,
                    model_name=config["GEMINI_MODEL_NAME"],
                    token=config["GEMINI_TOKEN"],
                    parse_format=config["JD_JSON_FORMAT"]
                )
                
                parser = JDParser(model=parser_gemini_model, mongo_config=mongo_config)
                jd_info_dict = parser.parseFromText(test_data[0]['full_text_jd'])
                jd_info_dict = parser.standardizeJDDict(jd_info_dict)
                print("Successfully processed sample data directly. Result:")
                print(json.dumps(jd_info_dict, indent=2))
    except Exception as inner_e:
        logging.error(f"Error processing sample data directly: {str(inner_e)}")