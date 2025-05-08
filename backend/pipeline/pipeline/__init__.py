import os
from dotenv import load_dotenv
from pathlib import Path
from dagster import Definitions, asset
from .assets.crawling_layer import test_layer_asset
from .assets.parser_layer import parse_jds_to_mongodb
from .ai_models.Parser import JDParser
from .ai_models.ParserModel import JDParserModel
from .ai_models.mongo_db_manager import MongoDBManager
from langchain_google_genai import ChatGoogleGenerativeAI


# Load environment variables
dotenv_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path)
# Create configurations
config = {
    "GEMINI_MODEL_NAME": os.getenv("GEMINI_MODEL_NAME"),
    "GEMINI_TOKEN": os.getenv("GEMINI_TOKEN"),
    "JD_JSON_FORMAT": str(os.getenv("JD_JSON_FORMAT")),
    "BATCH_SIZE": int(os.getenv("BATCH_SIZE", "5")),  # Default to 5 if not set
}
mongo_config = {
    "MONGODB_URI": os.getenv("MONGODB_URI"),
    "MONGODB_DATABASE": os.getenv("MONGODB_DATABASE"),
    "MONGODB_JD_COLLECTION": os.getenv("MONGODB_JD_COLLECTION"),
    "MONGODB_PARSED_JD_COLLECTION": os.getenv("MONGODB_PARSED_JD_COLLECTION")
}


@asset(
    name="hello_world_asset",
    description="A simple test asset to verify Dagster configuration",
    compute_kind="python",
    group_name="test"
)
def hello_world_asset():
    """A simple asset that returns a greeting message."""
    return "Hello, Dagster World!"

# Define the defs object at the module level - this is what Dagster looks for
defs = Definitions(
    assets=[hello_world_asset,
            test_layer_asset,
            parse_jds_to_mongodb,
            ],
    resources={
        "mongo_db_manager": MongoDBManager(mongo_config),
        "jd_parser": 
            JDParser(
                model=JDParserModel(
                    model_call=ChatGoogleGenerativeAI,
                    model_name=config["GEMINI_MODEL_NAME"],
                    token=config["GEMINI_TOKEN"],
                    parse_format=config["JD_JSON_FORMAT"]
                ),
                mongo_config=mongo_config,
            ),
        "processed_count": JDParser.batch_parse_jds_from_mongodb(mongo_config=mongo_config, batch_size=config["BATCH_SIZE"], config=config),
        "mongo_config": mongo_config,
        "config": config,
    }
)
