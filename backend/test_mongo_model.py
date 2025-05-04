import json
import logging
import os
from ai_models.Parser import JDParser, MongoDBManager
from langchain_google_genai import ChatGoogleGenerativeAI
from ai_models.ParserModel import JDParserModel
from config import config

# Cấu hình log để xem thông tin chi tiết
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Đường dẫn dữ liệu test
test_data_path = os.path.join(os.path.dirname(__file__), "ai_models/test_data.json")

# Kiểm tra kết nối MongoDB
try:
    mongo_manager = MongoDBManager.get_instance()
    db = mongo_manager.get_database()
    logging.info(f"Kết nối MongoDB thành công. Database: {config.MONGODB_DATABASE}")
except Exception as e:
    logging.error(f"Lỗi kết nối MongoDB: {str(e)}")

# Kiểm tra số lượng tài liệu trong collection
try:
    jd_collection = mongo_manager.get_collection(config.MONGODB_JD_COLLECTION)
    count = jd_collection.count_documents({})
    logging.info(f"Số lượng JD trong collection '{config.MONGODB_JD_COLLECTION}': {count}")
    
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

# Xử lý JDs từ MongoDB
try:
    logging.info("Bắt đầu xử lý JDs từ MongoDB...")
    processed_count = JDParser.batch_parse_jds_from_mongodb(batch_size=2)
    print(f"Đã xử lý thành công {processed_count} mô tả công việc")
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
                    model_name=config.GEMINI_MODEL_NAME,
                    token=config.GEMINI_TOKEN,
                    parse_format=config.JD_JSON_FORMAT
                )
                
                parser = JDParser(model=parser_gemini_model)
                jd_info_dict = parser.parseFromText(test_data[0]['full_text_jd'])
                jd_info_dict = parser.standardizeJDDict(jd_info_dict)
                print("Xử lý trực tiếp dữ liệu mẫu thành công. Kết quả:")
                print(json.dumps(jd_info_dict, indent=2))
    except Exception as inner_e:
        logging.error(f"Lỗi khi xử lý dữ liệu mẫu trực tiếp: {str(inner_e)}")