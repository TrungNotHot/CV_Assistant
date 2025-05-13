import os
import polars as pl
from dagster import asset, Output, StaticPartitionsDefinition, AssetIn
import uuid
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
    name="gold_job_description_table",
    description="Gold layer for Job table containing core job information",
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "cv_assistant"],
    compute_kind="Data Transformation",
    group_name="gold",
    partitions_def=MONTHLY,
    ins={
        "silver_job_descriptions": AssetIn(key=["silver", "cv_assistant", "silver_job_descriptions"]),
        "gold_job_position_table": AssetIn(key=["gold", "cv_assistant", "gold_job_position_table"]),
    },
)
def gold_job_description_table(context, silver_job_descriptions, gold_job_position_table):
    """
    Create Gold layer table for Job entity.
    Includes core job information fields and position_id as foreign key.
    """
    if silver_job_descriptions.is_empty():
        context.log.info("Input DataFrame is empty, skipping transformation")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "empty_input"
            }
        )
    
    # Create a dataframe with all job data excluding job_position (will be replaced with position_id)
    job_df = silver_job_descriptions.select(
        [
            pl.col("_id").alias("id"),  # Use existing _id as the primary key
            "job_title",
            "company_name",
            "posted_date", 
            "url", 
            "update_date", 
            "salary", 
            "location",
            "seniority_level"
        ]
    ).explode("seniority_level")
    
    # Get position mapping for jobs
    positions_mapping = silver_job_descriptions.filter(
        pl.col("job_position").is_not_null()
    ).select(
        [
            pl.col("_id").alias("id"),  
            pl.col("job_position").alias("position_name")
        ]
    )
    
    # Join with position reference table to get position_id
    if not gold_job_position_table.is_empty() and not positions_mapping.is_empty():
        positions_with_ids = positions_mapping.join(
            gold_job_position_table.select([
                pl.col("id").alias("position_id"), 
                pl.col("name").alias("position_name")
            ]),
            on="position_name",
            how="left"
        ).select(["id", "position_id"])
        
        # Join position_id back to job table
        job_df = job_df.join(
            positions_with_ids,
            on="id",
            how="left"
        )
        
        context.log.info(f"Joined {positions_with_ids.filter(pl.col('position_id').is_not_null()).height} jobs with position IDs")
    else:
        # If no positions available, add empty position_id column
        job_df = job_df.with_columns(pl.lit(None).alias("position_id"))
        context.log.info("No position data available to join")
    
    return Output(
        job_df,
        metadata={
            "row_count": job_df.shape[0],
            "column_count": job_df.shape[1],
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )

@asset(
    name="gold_tech_skill_table",
    description="Gold layer for TechSkill table containing technical skills",
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "cv_assistant"],
    compute_kind="Data Transformation",
    group_name="gold",
    partitions_def=MONTHLY,
    ins={
        "silver_job_descriptions": AssetIn(key=["silver", "cv_assistant", "silver_job_descriptions"]),
    },
)
def gold_tech_skill_table(context, silver_job_descriptions):
    """
    Create Gold layer table for TechSkill entity.
    Contains unique tech skills with their own IDs.
    """
    if silver_job_descriptions.is_empty():
        context.log.info("Input DataFrame is empty, skipping transformation")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "empty_input"
            }
        )
    
    # Filter to only jobs that have tech_stack
    jobs_with_tech = silver_job_descriptions.filter(pl.col("tech_stack").is_not_null())
    
    if jobs_with_tech.is_empty():
        context.log.info("No jobs with tech skills found")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "no_tech_skills"
            }
        )
    
    # Explode the tech_stack array to get all individual skills
    tech_skills = jobs_with_tech.select(
        pl.col("tech_stack")
    ).explode("tech_stack")
    
    # Get unique tech skills and assign IDs
    unique_tech_skills = tech_skills.unique().with_columns(
        pl.lit(pl.Series(name="id", values=[str(uuid.uuid4()) for _ in range(tech_skills.unique().height)]))
    )
    
    # Rename for clarity
    unique_tech_skills = unique_tech_skills.rename({"tech_stack": "name"})
    
    return Output(
        unique_tech_skills,
        metadata={
            "row_count": unique_tech_skills.shape[0],
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )

@asset(
    name="gold_job_tech_skill_mapping",
    description="Gold layer for Job-TechSkill many-to-many relationship",
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "cv_assistant"],
    compute_kind="Data Transformation",
    group_name="gold",
    partitions_def=MONTHLY,
    ins={
        "silver_job_descriptions": AssetIn(key=["silver", "cv_assistant", "silver_job_descriptions"]),
        "gold_tech_skill_table": AssetIn(key=["gold", "cv_assistant", "gold_tech_skill_table"]),
    },
)
def gold_job_tech_skill_mapping(context, silver_job_descriptions, gold_tech_skill_table):
    """
    Create Gold layer mapping table for Job-TechSkill relationships.
    Represents the many-to-many relationship between jobs and tech skills.
    """
    if silver_job_descriptions.is_empty() or gold_tech_skill_table.is_empty():
        context.log.info("Input DataFrame is empty, skipping transformation")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "empty_input"
            }
        )
    
    # Filter to only jobs that have tech_stack
    jobs_with_tech = silver_job_descriptions.filter(pl.col("tech_stack").is_not_null())
    
    if jobs_with_tech.is_empty():
        context.log.info("No jobs with tech skills found")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "no_tech_skills"
            }
        )
    
    # Explode the tech_stack array to create individual job-skill pairs
    job_tech_pairs = jobs_with_tech.select(
        [
            pl.col("_id").alias("job_id"),  # Job ID reference
            pl.col("tech_stack").alias("skill_name")
        ]
    ).explode("skill_name")
    
    # Join with tech skill table to get the tech_skill_id
    job_tech_mapping = job_tech_pairs.join(
        gold_tech_skill_table.select([
            pl.col("id").alias("tech_skill_id"), 
            pl.col("name").alias("skill_name")
        ]),
        on="skill_name"
    ).select(["job_id", "tech_skill_id"])
    
    return Output(
        job_tech_mapping,
        metadata={
            "row_count": job_tech_mapping.shape[0],
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )

@asset(
    name="gold_soft_skill_table",
    description="Gold layer for SoftSkill table containing soft skills",
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "cv_assistant"],
    compute_kind="Data Transformation",
    group_name="gold",
    partitions_def=MONTHLY,
    ins={
        "silver_job_descriptions": AssetIn(key=["silver", "cv_assistant", "silver_job_descriptions"]),
    },
)
def gold_soft_skill_table(context, silver_job_descriptions):
    """
    Create Gold layer table for SoftSkill entity.
    Contains unique soft skills with their own IDs.
    """
    if silver_job_descriptions.is_empty():
        context.log.info("Input DataFrame is empty, skipping transformation")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "empty_input"
            }
        )
    
    # Filter to only jobs that have soft_skills
    jobs_with_soft_skills = silver_job_descriptions.filter(pl.col("soft_skills").is_not_null())
    
    if jobs_with_soft_skills.is_empty():
        context.log.info("No jobs with soft skills found")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "no_soft_skills"
            }
        )
    
    # Explode the soft_skills array to get all individual skills
    soft_skills = jobs_with_soft_skills.select(
        pl.col("soft_skills")
    ).explode("soft_skills")
    
    # Get unique soft skills and assign IDs
    unique_soft_skills = soft_skills.unique().with_columns(
        pl.lit(pl.Series(name="id", values=[str(uuid.uuid4()) for _ in range(soft_skills.unique().height)]))
    )
    
    # Rename for clarity
    unique_soft_skills = unique_soft_skills.rename({"soft_skills": "name"})
    
    return Output(
        unique_soft_skills,
        metadata={
            "row_count": unique_soft_skills.shape[0],
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )

@asset(
    name="gold_job_soft_skill_mapping",
    description="Gold layer for Job-SoftSkill many-to-many relationship",
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "cv_assistant"],
    compute_kind="Data Transformation",
    group_name="gold",
    partitions_def=MONTHLY,
    ins={
        "silver_job_descriptions": AssetIn(key=["silver", "cv_assistant", "silver_job_descriptions"]),
        "gold_soft_skill_table": AssetIn(key=["gold", "cv_assistant", "gold_soft_skill_table"]),
    },
)
def gold_job_soft_skill_mapping(context, silver_job_descriptions, gold_soft_skill_table):
    """
    Create Gold layer mapping table for Job-SoftSkill relationships.
    Represents the many-to-many relationship between jobs and soft skills.
    """
    if silver_job_descriptions.is_empty() or gold_soft_skill_table.is_empty():
        context.log.info("Input DataFrame is empty, skipping transformation")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "empty_input"
            }
        )
    
    # Filter to only jobs that have soft_skills
    jobs_with_soft_skills = silver_job_descriptions.filter(pl.col("soft_skills").is_not_null())
    
    if jobs_with_soft_skills.is_empty():
        context.log.info("No jobs with soft skills found")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "no_soft_skills"
            }
        )
    
    # Explode the soft_skills array to create individual job-skill pairs
    job_soft_skill_pairs = jobs_with_soft_skills.select(
        [
            pl.col("_id").alias("job_id"),  # Job ID reference
            pl.col("soft_skills").alias("skill_name")
        ]
    ).explode("skill_name")
    
    # Join with soft skill table to get the soft_skill_id
    job_soft_skill_mapping = job_soft_skill_pairs.join(
        gold_soft_skill_table.select([
            pl.col("id").alias("soft_skill_id"), 
            pl.col("name").alias("skill_name")
        ]),
        on="skill_name"
    ).select(["job_id", "soft_skill_id"])
    
    return Output(
        job_soft_skill_mapping,
        metadata={
            "row_count": job_soft_skill_mapping.shape[0],
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )

@asset(
    name="gold_major_table",
    description="Gold layer for Major table containing major/education fields",
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "cv_assistant"],
    compute_kind="Data Transformation",
    group_name="gold",
    partitions_def=MONTHLY,
    ins={
        "silver_job_descriptions": AssetIn(key=["silver", "cv_assistant", "silver_job_descriptions"]),
    },
)
def gold_major_table(context, silver_job_descriptions):
    """
    Create Gold layer table for Major entity.
    Contains unique majors with their own IDs.
    """
    if silver_job_descriptions.is_empty():
        context.log.info("Input DataFrame is empty, skipping transformation")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "empty_input"
            }
        )
    
    # Filter to only jobs that have major
    jobs_with_majors = silver_job_descriptions.filter(pl.col("major").is_not_null())
    
    if jobs_with_majors.is_empty():
        context.log.info("No jobs with majors found")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "no_majors"
            }
        )
    
    # Explode the major array to get all individual majors
    majors = jobs_with_majors.select(
        pl.col("major")
    ).explode("major")
    
    # Get unique majors and assign IDs
    unique_majors = majors.unique().with_columns(
        pl.lit(pl.Series(name="id", values=[str(uuid.uuid4()) for _ in range(majors.unique().height)]))
    )
    
    # Rename for clarity
    unique_majors = unique_majors.rename({"major": "name"})
    
    return Output(
        unique_majors,
        metadata={
            "row_count": unique_majors.shape[0],
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )

@asset(
    name="gold_job_major_mapping",
    description="Gold layer for Job-Major many-to-many relationship",
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "cv_assistant"],
    compute_kind="Data Transformation",
    group_name="gold",
    partitions_def=MONTHLY,
    ins={
        "silver_job_descriptions": AssetIn(key=["silver", "cv_assistant", "silver_job_descriptions"]),
        "gold_major_table": AssetIn(key=["gold", "cv_assistant", "gold_major_table"]),
    },
)
def gold_job_major_mapping(context, silver_job_descriptions, gold_major_table):
    """
    Create Gold layer mapping table for Job-Major relationships.
    Represents the many-to-many relationship between jobs and majors.
    """
    if silver_job_descriptions.is_empty() or gold_major_table.is_empty():
        context.log.info("Input DataFrame is empty, skipping transformation")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "empty_input"
            }
        )
    
    # Filter to only jobs that have major
    jobs_with_majors = silver_job_descriptions.filter(pl.col("major").is_not_null())
    
    if jobs_with_majors.is_empty():
        context.log.info("No jobs with majors found")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "no_majors"
            }
        )
    
    # Explode the major array to create individual job-major pairs
    job_major_pairs = jobs_with_majors.select(
        [
            pl.col("_id").alias("job_id"),  # Job ID reference
            pl.col("major").alias("major_name")
        ]
    ).explode("major_name")
    
    # Join with major table to get the major_id
    job_major_mapping = job_major_pairs.join(
        gold_major_table.select([
            pl.col("id").alias("major_id"), 
            pl.col("name").alias("major_name")
        ]),
        on="major_name"
    ).select(["job_id", "major_id"])
    
    return Output(
        job_major_mapping,
        metadata={
            "row_count": job_major_mapping.shape[0],
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )

@asset(
    name="gold_job_position_table",
    description="Gold layer for JobPosition table containing standardized job positions",
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "cv_assistant"],
    compute_kind="Data Transformation",
    group_name="gold",
    partitions_def=MONTHLY,
    ins={
        "silver_job_descriptions": AssetIn(key=["silver", "cv_assistant", "silver_job_descriptions"]),
    },
)
def gold_job_position_table(context, silver_job_descriptions):
    """
    Create Gold layer table for JobPosition entity.
    Contains unique job positions with their own IDs.
    Used as a reference table in the one-to-many relationship with job table.
    """
    if silver_job_descriptions.is_empty():
        context.log.info("Input DataFrame is empty, skipping transformation")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "empty_input"
            }
        )
    
    # Filter to only jobs that have job_position
    jobs_with_positions = silver_job_descriptions.filter(pl.col("job_position").is_not_null())
    
    if jobs_with_positions.is_empty():
        context.log.info("No jobs with positions found")
        return Output(
            pl.DataFrame(),
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "no_positions"
            }
        )
    
    # Extract unique job positions
    positions = jobs_with_positions.select(
        pl.col("job_position")
    ).unique()
    
    # Get unique positions and assign IDs
    unique_positions = positions.with_columns(
        pl.lit(pl.Series(name="id", values=[str(uuid.uuid4()) for _ in range(positions.height)]))
    )
    
    # Rename for clarity
    unique_positions = unique_positions.rename({"job_position": "name"})
    
    return Output(
        unique_positions,
        metadata={
            "row_count": unique_positions.shape[0],
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )