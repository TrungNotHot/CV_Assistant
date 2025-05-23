from dagster import asset, Output, StaticPartitionsDefinition, Config
import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Optional, List, Sequence
import json
import pandas as pd

def generate_monthly_partitions():
    # Start from one year ago, using the first day of the month
    start_date = datetime.strptime("2025-01-01", "%Y-%m-%d")
    # End at current date plus 1 months for future partitioning, using first day of month
    end_date = (datetime.now() + relativedelta(months=1)).replace(day=1)
    partitions = []
    current = start_date
    
    while current <= end_date:
        partitions.append(current.strftime("%Y-%m-%d"))
        current = (current + relativedelta(months=1)).replace(day=1)
    
    return partitions
MONTHLY = StaticPartitionsDefinition(generate_monthly_partitions())

@asset(
    name="bronze_job_descriptions",
    description="Extract data from MongoDB and load into MinIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mongo_db_manager"},
    key_prefix=["bronze", "cv_assistant"],
    compute_kind="MongoDB",
    group_name="bronze",
    partitions_def=MONTHLY,
)
def bronze_job_descriptions(context) -> Output[pl.DataFrame]:
    partition_by = "posted_date"
    partition = context.asset_partition_key_for_output()
    partition_date = datetime.strptime(partition, "%Y-%m-%d")
    next_partition_date = partition_date + relativedelta(months=1)
    query = {
        f"{partition_by}": {
            "$gte": partition_date.strftime("%Y-%m-%d"),
            "$lt": next_partition_date.strftime("%Y-%m-%d")
        }
    }
    
    # First check count to avoid unnecessary processing
    record_count = context.resources.mongo_db_manager.count_documents(
        collection_name="parsed_job_descriptions", 
        query=query
    )
    
    context.log.info(f"Found {record_count} records for partition {partition}")
    
    if record_count == 0:
        context.log.info(f"No data found for partition {partition} (month). Returning empty DataFrame.")
        return Output(
            pl.DataFrame(),
            metadata={
                "collection": "job_descriptions",
                "row_count": 0,
                "partition": partition,
                "status": "empty"
            },
        )
    
    # Get data from MongoDB using extract_data with batch processing
    # MongoDB ObjectId will be automatically processed by mongo_db_manager
    df_data = context.resources.mongo_db_manager.extract_data(
        collection_name="parsed_job_descriptions", 
        query=query,
        batch_size=500
    )
    
    context.log.info(f"Collection extracted with shape: {df_data.shape}")
    
    # Close MongoDB connection after successfully extracting data
    context.resources.mongo_db_manager.close_connection()
    
    return Output(
        df_data,
        metadata={
            "collection": "job_descriptions",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": str(df_data.columns),
            "partition": partition,
            "status": "success"
        },
    )
