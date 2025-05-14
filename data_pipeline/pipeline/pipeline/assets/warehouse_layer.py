from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
import polars as pl
from polars import DataFrame
from datetime import datetime
from dateutil.relativedelta import relativedelta

def generate_monthly_partitions():
    start_date = datetime.strptime("2025-01-01", "%Y-%m-%d")
    end_date = (datetime.now() + relativedelta(months=1)).replace(day=1)
    partitions = []
    current = start_date
    
    while current <= end_date:
        partitions.append(current.strftime("%Y-%m-%d"))
        current = (current + relativedelta(months=1)).replace(day=1)
    
    return partitions
MONTHLY = StaticPartitionsDefinition(generate_monthly_partitions())


@asset(
    name='warehouse_job_description_table',
    description="Load gold_job_description_table from Minio to PostgreSQL",
    ins={
        "gold_job_description_table": AssetIn(
            key_prefix=["gold", "cv_assistant"],
        ),
    },
    metadata={
        "database": "cv_assistant",
        "schema": "warehouse",
        "table": "Job",
        "primary_keys": ["id"],
        "columns": ["id", "job_title", "company_name", "posted_date", "url", "update_date", "salary", "location", "seniority_level", "position_id"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "cv_assistant"],  
    compute_kind="Postgres",
    group_name="warehouse",
    partitions_def=MONTHLY,
)
def warehouse_job_description_table(context, gold_job_description_table: DataFrame):
    context.log.info("Transferring job data from gold layer to PostgreSQL warehouse")

    return Output(
        gold_job_description_table,
        metadata={
            "database": "cv_assistant",
            "schema": "warehouse",
            "table": "Job",
            "primary_keys": ["id", "seniority_level"],
            "row_count": gold_job_description_table.shape[0],
        },
    )


@asset(
    name='warehouse_tech_skill_table',
    description="Load gold_tech_skill_table from Minio to PostgreSQL",
    ins={
        "gold_tech_skill_table": AssetIn(
            key_prefix=["gold", "cv_assistant"],
        ),
    },
    metadata={
        "database": "cv_assistant",
        "schema": "warehouse",
        "table": "TechSkill",
        "primary_keys": ["id"],
        "columns": ["id", "name"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "cv_assistant"],    
    compute_kind="Postgres",
    group_name="warehouse",
    partitions_def=MONTHLY,
)
def warehouse_tech_skill_table(context, gold_tech_skill_table: DataFrame):
    context.log.info("Transferring tech skill data from gold layer to PostgreSQL warehouse")

    return Output(
        gold_tech_skill_table,
        metadata={
            "database": "cv_assistant",
            "schema": "warehouse",
            "table": "TechSkill",
            "primary_keys": ["id"],
            "row_count": gold_tech_skill_table.shape[0],
        },
    )


@asset(
    name='warehouse_job_tech_skill_mapping_table',
    description="Load gold_job_tech_skill_mapping from Minio to PostgreSQL",
    ins={
        "gold_job_tech_skill_mapping": AssetIn(
            key_prefix=["gold", "cv_assistant"],
        ),
    },
    metadata={
        "database": "cv_assistant",
        "schema": "warehouse",
        "table": "JobTechSkill",
        "primary_keys": ["job_id", "tech_skill_id"],
        "columns": ["job_id", "tech_skill_id"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "cv_assistant"],    
    compute_kind="Postgres",
    group_name="warehouse",
    partitions_def=MONTHLY,
)
def warehouse_job_tech_skill_mapping_table(context, gold_job_tech_skill_mapping: DataFrame):
    context.log.info("Transferring job-tech skill mappings from gold layer to PostgreSQL warehouse")

    return Output(
        gold_job_tech_skill_mapping,
        metadata={
            "database": "cv_assistant",
            "schema": "warehouse",
            "table": "JobTechSkill",
            "primary_keys": ["job_id", "tech_skill_id"],
            "row_count": gold_job_tech_skill_mapping.shape[0],
        },
    )


@asset(
    name='warehouse_soft_skill_table',
    description="Load gold_soft_skill_table from Minio to PostgreSQL",
    ins={
        "gold_soft_skill_table": AssetIn(
            key_prefix=["gold", "cv_assistant"],
        ),
    },
    metadata={
        "database": "cv_assistant",
        "schema": "warehouse",
        "table": "SoftSkill",
        "primary_keys": ["id"],
        "columns": ["id", "name"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "cv_assistant"],    
    compute_kind="Postgres",
    group_name="warehouse",
    partitions_def=MONTHLY,
)
def warehouse_soft_skill_table(context, gold_soft_skill_table: DataFrame):
    context.log.info("Transferring soft skill data from gold layer to PostgreSQL warehouse")

    return Output(
        gold_soft_skill_table,
        metadata={
            "database": "cv_assistant",
            "schema": "warehouse",
            "table": "SoftSkill",
            "primary_keys": ["id"],
            "row_count": gold_soft_skill_table.shape[0],
        },
    )


@asset(
    name='warehouse_job_soft_skill_mapping_table',
    description="Load gold_job_soft_skill_mapping from Minio to PostgreSQL",
    ins={
        "gold_job_soft_skill_mapping": AssetIn(
            key_prefix=["gold", "cv_assistant"],
        ),
    },
    metadata={
        "database": "cv_assistant",
        "schema": "warehouse",
        "table": "JobSoftSkill",
        "primary_keys": ["job_id", "soft_skill_id"],
        "columns": ["job_id", "soft_skill_id"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "cv_assistant"],    
    compute_kind="Postgres",
    group_name="warehouse",
    partitions_def=MONTHLY,
)
def warehouse_job_soft_skill_mapping_table(context, gold_job_soft_skill_mapping: DataFrame):
    context.log.info("Transferring job-soft skill mappings from gold layer to PostgreSQL warehouse")

    return Output(
        gold_job_soft_skill_mapping,
        metadata={
            "database": "cv_assistant",
            "schema": "warehouse",
            "table": "JobSoftSkill",
            "primary_keys": ["job_id", "soft_skill_id"],
            "row_count": gold_job_soft_skill_mapping.shape[0],
        },
    )


@asset(
    name='warehouse_major_table',
    description="Load gold_major_table from Minio to PostgreSQL",
    ins={
        "gold_major_table": AssetIn(
            key_prefix=["gold", "cv_assistant"],
        ),
    },
    metadata={
        "database": "cv_assistant",
        "schema": "warehouse",
        "table": "Major",
        "primary_keys": ["id"],
        "columns": ["id", "name"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "cv_assistant"],    
    compute_kind="Postgres",
    group_name="warehouse",
    partitions_def=MONTHLY,
)
def warehouse_major_table(context, gold_major_table: DataFrame):
    context.log.info("Transferring major data from gold layer to PostgreSQL warehouse")

    return Output(
        gold_major_table,
        metadata={
            "database": "cv_assistant",
            "schema": "warehouse",
            "table": "Major",
            "primary_keys": ["id"],
            "row_count": gold_major_table.shape[0],
        },
    )


@asset(
    name='warehouse_job_major_mapping_table',
    description="Load gold_job_major_mapping from Minio to PostgreSQL",
    ins={
        "gold_job_major_mapping": AssetIn(
            key_prefix=["gold", "cv_assistant"],
        ),
    },
    metadata={
        "database": "cv_assistant",
        "schema": "warehouse",
        "table": "JobMajor",
        "primary_keys": ["job_id", "major_id"],
        "columns": ["job_id", "major_id"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "cv_assistant"],    
    compute_kind="Postgres",
    group_name="warehouse",
    partitions_def=MONTHLY,
)
def warehouse_job_major_mapping_table(context, gold_job_major_mapping: DataFrame):
    context.log.info("Transferring job-major mappings from gold layer to PostgreSQL warehouse")

    return Output(
        gold_job_major_mapping,
        metadata={
            "database": "cv_assistant",
            "schema": "warehouse",
            "table": "JobMajor",
            "primary_keys": ["job_id", "major_id"],
            "row_count": gold_job_major_mapping.shape[0],
        },
    )


@asset(
    name='warehouse_job_position_table',
    description="Load gold_job_position_table from Minio to PostgreSQL",
    ins={
        "gold_job_position_table": AssetIn(
            key_prefix=["gold", "cv_assistant"],
        ),
    },
    metadata={
        "database": "cv_assistant",
        "schema": "warehouse",
        "table": "JobPosition",
        "primary_keys": ["id"],
        "columns": ["id", "name"],
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "cv_assistant"],    
    compute_kind="Postgres",
    group_name="warehouse",
    partitions_def=MONTHLY,
)
def warehouse_job_position_table(context, gold_job_position_table: DataFrame):
    context.log.info("Transferring job position data from gold layer to PostgreSQL warehouse")

    return Output(
        gold_job_position_table,
        metadata={
            "database": "cv_assistant",
            "schema": "warehouse",
            "table": "JobPosition",
            "primary_keys": ["id"],
            "row_count": gold_job_position_table.shape[0],
        },
    )