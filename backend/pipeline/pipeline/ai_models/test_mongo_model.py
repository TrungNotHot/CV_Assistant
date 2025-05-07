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
 
# Cấu hình log để xem thông tin chi tiết
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Đường dẫn dữ liệu test
test_data_path = os.path.join("/home/trungnothot/Study/CV_Assistant/backend/test/test_data.json")

# Kiểm tra kết nối MongoDB
try:
    mongo_manager = MongoDBManager.get_instance(mongo_config)
    db = mongo_manager.get_database()
    logging.info(f"Kết nối MongoDB thành công. Database: {mongo_config['MONGODB_DATABASE']}")
except Exception as e:
    logging.error(f"Lỗi kết nối MongoDB: {str(e)}")

# Kiểm tra số lượng tài liệu trong collection
try:
    jd_collection = mongo_manager.get_collection(mongo_config["MONGODB_JD_COLLECTION"])
    count = jd_collection.count_documents({})
    logging.info(f"Số lượng JD trong collection '{mongo_config['MONGODB_JD_COLLECTION']}': {count}")
    
    if count == 0:
        logging.info("Không có dữ liệu trong collection. Thử thêm dữ liệu mẫu.")
        
        # Đọc và thêm dữ liệu mẫu vào MongoDB nếu không có dữ liệu
        if os.path.exists(test_data_path):
            with open(test_data_path, 'r') as f:
                test_data = json.load(f)
                
            # Thêm _id cho mỗi bản ghi nếu chưa có
            for i, item in enumerate(test_data):
                if '_id' not in item:
                    item['_id'] = f"test_jd_{i}"
            
            # Thêm vào MongoDB
            try:
                result = jd_collection.insert_many(test_data)
                logging.info(f"Đã thêm {len(result.inserted_ids)} bản ghi mẫu vào MongoDB")
            except Exception as e:
                logging.error(f"Lỗi khi thêm dữ liệu mẫu: {str(e)}")
except Exception as e:
    logging.error(f"Lỗi khi kiểm tra collection: {str(e)}")

# Xử lý tất cả các JDs từ MongoDB theo batch
try:
    # Lấy số lượng JD chưa xử lý
    parser_gemini_model = JDParserModel(
        model_call=ChatGoogleGenerativeAI,
        model_name=config["GEMINI_MODEL_NAME"],
        token=config["GEMINI_TOKEN"],
        parse_format=config["JD_JSON_FORMAT"]
    )
    parser = JDParser(model=parser_gemini_model, mongo_config=mongo_config)
    
    # Lấy danh sách các JD chưa được parse
    unparsed_jds = parser.fetch_unparsed_jds_from_mongodb()
    total_jds = len(unparsed_jds)
    
    if total_jds == 0:
        logging.info("Không có JD nào cần xử lý")
    else:
        batch_size = config["BATCH_SIZE"]
        logging.info(f"Tìm thấy {total_jds} JD cần xử lý, với batch_size = {batch_size}")
        
        # Tính số lượng batch
        num_batches = (total_jds + batch_size - 1) // batch_size  # Làm tròn lên
        
        # Xử lý theo từng batch
        total_processed = 0
        for batch_num in range(num_batches):
            logging.info(f"Đang xử lý batch {batch_num + 1}/{num_batches}...")
            try:
                processed_count = JDParser.batch_parse_jds_from_mongodb(mongo_config=mongo_config, batch_size=batch_size)
                total_processed += processed_count
                logging.info(f"Đã xử lý {processed_count} JD trong batch {batch_num + 1}. Tổng số đã xử lý: {total_processed}/{total_jds}")
            except Exception as batch_e:
                logging.error(f"Lỗi khi xử lý batch {batch_num + 1}: {str(batch_e)}")
        
        logging.info(f"Hoàn thành! Đã xử lý tổng cộng {total_processed} JD")
        
except Exception as e:
    logging.error(f"Lỗi khi xử lý JDs: {str(e)}")
    
    # Nếu xử lý từ MongoDB thất bại, thử xử lý dữ liệu mẫu trực tiếp
    try:
        logging.info("Thử xử lý dữ liệu mẫu trực tiếp...")
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
                print("Xử lý trực tiếp dữ liệu mẫu thành công. Kết quả:")
                print(json.dumps(jd_info_dict, indent=2))
    except Exception as inner_e:
        logging.error(f"Lỗi khi xử lý dữ liệu mẫu trực tiếp: {str(inner_e)}")