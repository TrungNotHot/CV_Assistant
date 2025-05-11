import os
from dotenv import load_dotenv
from pathlib import Path
from dagster import Definitions
from langchain_google_genai import ChatGoogleGenerativeAI
from .assets.parser_layer import parse_jds_to_mongodb
from .assets.bronze_layer import bronze_job_descriptions
from .assets.silver_layer import normalize_location, normalize_major, normalize_soft_skills, normalize_tech_stack, silver_jobs_df

from .resources.Parser import JDParser
from .resources.ParserModel import JDParserModel
from .resources.mongo_db_manager import MongoDBManager
from .resources.minio_io_manager import MinIOIOManager



# Load environment variables
dotenv_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path)
# Create configurations
GEMINI_CONFIG = {
    "GEMINI_MODEL_NAME": os.getenv("GEMINI_MODEL_NAME"),
    "GEMINI_TOKEN": os.getenv("GEMINI_TOKEN"),
    "JD_JSON_FORMAT": str(os.getenv("JD_JSON_FORMAT")),
    "BATCH_SIZE": int(os.getenv("BATCH_SIZE", "5")),  # Default to 5 if not set
}
MONGO_CONFIG = {
    "MONGODB_URI": os.getenv("MONGODB_URI"),
    "MONGODB_DATABASE": os.getenv("MONGODB_DATABASE"),
    "MONGODB_JD_COLLECTION": os.getenv("MONGODB_JD_COLLECTION"),
    "MONGODB_PARSED_JD_COLLECTION": os.getenv("MONGODB_PARSED_JD_COLLECTION")
}
MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY"),
    "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY"),
}

# Define the defs object at the module level - this is what Dagster looks for
defs = Definitions(
    assets=[
            parse_jds_to_mongodb,
            bronze_job_descriptions,
            normalize_location,
            normalize_major,
            normalize_soft_skills,
            normalize_tech_stack,
            silver_jobs_df
            ],
    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "mongo_db_manager": MongoDBManager(MONGO_CONFIG),
        "jd_parser": 
            JDParser(
                model=JDParserModel(
                    model_call=ChatGoogleGenerativeAI,
                    model_name=GEMINI_CONFIG["GEMINI_MODEL_NAME"],
                    token=GEMINI_CONFIG["GEMINI_TOKEN"],
                    parse_format=GEMINI_CONFIG["JD_JSON_FORMAT"]
                ),
                mongo_config=MONGO_CONFIG,
            ),
        "processed_count": JDParser.batch_parse_jds_from_mongodb(mongo_config=MONGO_CONFIG, batch_size=GEMINI_CONFIG["BATCH_SIZE"], config=GEMINI_CONFIG),
        "mongo_config": MONGO_CONFIG,
        "config": GEMINI_CONFIG,
    }
)
