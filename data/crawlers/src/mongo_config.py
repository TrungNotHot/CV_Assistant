import os
from dotenv import load_dotenv
from pymongo import MongoClient

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(BASE_DIR, ".env")

if not os.path.exists(dotenv_path):
    print(f"⚠️  [WARNING] .env file not found at {dotenv_path}")
else:
    load_dotenv(dotenv_path)

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise EnvironmentError("❌ MONGO_URI not found in environment variables or .env file")

MONGO_DB = os.getenv("MONGO_DB", "crawl-data")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "jd")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]
collection.create_index("hash", unique=True)
