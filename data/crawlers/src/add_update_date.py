from datetime import datetime, timezone
from mongo_config import collection

result = collection.update_many(
    {},  
    {"$set": {"update_date": datetime.now(timezone.utc)}}
)

print(f"✅ Updated {result.modified_count} documents with 'update_date'")