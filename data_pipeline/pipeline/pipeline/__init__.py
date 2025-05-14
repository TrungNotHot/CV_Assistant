import os
from dotenv import load_dotenv
from pathlib import Path
from dagster import Definitions
from dagster import in_process_executor
from langchain_google_genai import ChatGoogleGenerativeAI
from .assets.parser_layer import parse_jds_to_mongodb
from .assets.bronze_layer import bronze_job_descriptions
from .assets.silver_layer import normalize_location, normalize_major, normalize_soft_skills, normalize_tech_stack, normalize_job_position, silver_job_descriptions
from .assets.gold_layer import (
    gold_job_description_table, 
    gold_tech_skill_table, 
    gold_job_tech_skill_mapping, 
    gold_soft_skill_table, 
    gold_job_soft_skill_mapping, 
    gold_major_table, 
    gold_job_major_mapping,
    gold_job_position_table
)
from .assets.warehouse_layer import (
    warehouse_job_description_table,
    warehouse_tech_skill_table,
    warehouse_job_tech_skill_mapping_table,
    warehouse_soft_skill_table,
    warehouse_job_soft_skill_mapping_table,
    warehouse_major_table,
    warehouse_job_major_mapping_table,
    warehouse_job_position_table
)
from dagster import ScheduleDefinition, AssetSelection, define_asset_job
from .resources.Parser import JDParser
from .resources.ParserModel import JDParserModel
from .resources.mongo_db_manager import MongoDBManager
from .resources.minio_io_manager import MinIOIOManager
from .resources.psql_io_manager import PostgreSQLIOManager

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
POSTGRESQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

# Define jobs based on asset selection
parse_job = define_asset_job(
    name="parse_jds_job",
    selection=AssetSelection.assets(parse_jds_to_mongodb)
)

sequential_job_in_datalake = define_asset_job(
    name="sequential_job_in_datalake",
    selection=AssetSelection.assets(
        # Đặt theo đúng thứ tự muốn chạy
        parse_jds_to_mongodb,
        bronze_job_descriptions,
        normalize_location,
        normalize_major,
        normalize_soft_skills,
        normalize_tech_stack,
        normalize_job_position,
        silver_job_descriptions,
        gold_job_position_table,
        gold_tech_skill_table,
        gold_soft_skill_table,
        gold_major_table,
        gold_job_description_table,
        gold_job_tech_skill_mapping,
        gold_job_soft_skill_mapping,
        gold_job_major_mapping,
    ),
    executor_def=in_process_executor
)

load_to_warehouse_job = define_asset_job(
    name="load_to_warehouse_job",
    selection=AssetSelection.assets(
        warehouse_job_description_table,
        warehouse_tech_skill_table,
        warehouse_job_tech_skill_mapping_table,
        warehouse_soft_skill_table,
        warehouse_job_soft_skill_mapping_table,
        warehouse_major_table,
        warehouse_job_major_mapping_table,
        warehouse_job_position_table
    ),
    executor_def=in_process_executor
)

# Define schedules
parse_schedule = ScheduleDefinition(
    job=parse_job,
    cron_schedule="0 0 * * 6",  # Run once a week on Saturday at midnight
    execution_timezone="UTC",
)

in_datalake_schedule = ScheduleDefinition(
    job=sequential_job_in_datalake,
    cron_schedule="0 0 * * 0",  # Run once a week on Sunday at midnight 
)

warehouse_schedule = ScheduleDefinition(
    job=load_to_warehouse_job,
    cron_schedule="0 0 * * 1",  # Run once a week on Monday at midnight
)

# Define the defs object at the module level - this is what Dagster looks for
defs = Definitions(
    assets=[
            # Parser layer assets
            parse_jds_to_mongodb,
            # Bronze layer assets
            bronze_job_descriptions,
            # Silver layer assets
            normalize_location,
            normalize_major,
            normalize_soft_skills,
            normalize_tech_stack,
            normalize_job_position,
            silver_job_descriptions,
            # Gold layer assets
            gold_job_description_table,
            gold_tech_skill_table,
            gold_job_tech_skill_mapping,
            gold_soft_skill_table,
            gold_job_soft_skill_mapping,
            gold_major_table,
            gold_job_major_mapping,
            gold_job_position_table,
            # Warehouse layer assets
            warehouse_job_description_table,
            warehouse_tech_skill_table,
            warehouse_job_tech_skill_mapping_table,
            warehouse_soft_skill_table,
            warehouse_job_soft_skill_mapping_table,
            warehouse_major_table,
            warehouse_job_major_mapping_table,
            warehouse_job_position_table,
            ],

    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "mongo_db_manager": MongoDBManager(MONGO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(POSTGRESQL_CONFIG),
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
    },
    schedules=[
        parse_schedule, 
        in_datalake_schedule,
        warehouse_schedule
    ],
)
