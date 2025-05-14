from dagster import asset, AssetExecutionContext

@asset(
    name="parse_jds_to_mongodb",
    description="Asset that parses JD data and saves it to MongoDB using batch processing",
    required_resource_keys={"mongo_db_manager", "jd_parser", "mongo_config", "config", "processed_count"},
    compute_kind="Python",
    group_name="parser"
)
def parse_jds_to_mongodb(context: AssetExecutionContext):
    """
    This asset extracts JD information from raw job descriptions and saves the parsed data to MongoDB.
    It uses the batch_parse_jds_from_mongodb static method from Parser.py for efficient batch processing.
    """
  
    try:
        # Check MongoDB connection
        mongo_manager = context.resources.mongo_db_manager
        mongo_config = context.resources.mongo_config
        config = context.resources.config
        context.log.info(f"MongoDB connection successful. Database: {mongo_config['MONGODB_DATABASE']}")
    except Exception as e:
        context.log.error(f"Error in parse_jds_to_mongodb: {str(e)}")
        return {"status": "error", "message": str(e)}
        
    try:
        # Check document count in collection
        jd_collection = mongo_manager.get_collection(mongo_config["MONGODB_JD_COLLECTION"])
        parsed_jd_collection = mongo_manager.get_collection(mongo_config["MONGODB_PARSED_JD_COLLECTION"])
        
        count_unparsed = jd_collection.count_documents({})
        count_parsed = parsed_jd_collection.count_documents({})
        context.log.info(f"Number of JDs in collection '{mongo_config['MONGODB_JD_COLLECTION']}': {count_unparsed}")
        context.log.info(f"Number of parsed JDs in collection '{mongo_config['MONGODB_PARSED_JD_COLLECTION']}': {count_parsed}")

        # Initialize the parser model
        parser = context.resources.jd_parser
        
        # Check if there are unparsed JDs
        unparsed_jds = parser.fetch_unparsed_jds_from_mongodb()
        total_jds = len(unparsed_jds)
        
        if total_jds == 0:
            context.log.info("No JDs need processing")
            return {"status": "success", "processed": 0}
        else:
            batch_size = config["BATCH_SIZE"]
            context.log.info(f"Found {total_jds} JDs to process, with batch_size = {batch_size}")
            
            # Calculate number of batches (use at least 1 batch)
            num_batches = (total_jds + batch_size - 1) // batch_size
            context.log.info(f"Number of batches to process: {num_batches}")
            
            # Use the static batch processing method
            total_processed = 0
            for batch_num in range(num_batches):
                context.log.info(f"Processing batch {batch_num + 1}/{num_batches}...")
                try:
                    # Call the static method to process this batch
                    processed = parser.batch_parse_jds_from_mongodb(
                        mongo_config=mongo_config,
                        batch_size=batch_size,
                        config=config
                    )
                    total_processed += processed
                    context.log.info(f"Batch {batch_num + 1} complete. Processed {processed} JDs. Total: {total_processed}")
                except Exception as batch_e:
                    context.log.error(f"Error processing batch {batch_num + 1}: {str(batch_e)}")
            
            context.log.info(f"Completed! Processed a total of {total_processed} JDs")
            return {"status": "success", "processed": total_processed}
    except Exception as e:
        context.log.error(f"Error processing JDs: {str(e)}")
        return {"status": "error", "message": str(e)}
    finally:
        # Close MongoDB connection
        mongo_manager.close_connection()
        context.log.info("MongoDB connection closed")
        context.log.info("JD parsing process completed")
        return {"status": "completed"}
