import os
import json
import re
import polars as pl
from dagster import asset, Output, StaticPartitionsDefinition, AssetIn
from functools import lru_cache
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

REFERENCE_FILE_PATH = os.path.join(os.path.dirname(__file__), "../resources/reference.json")
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


# UDF
@lru_cache(maxsize=1)
def load_reference_data():
    """Load reference data from JSON file with caching for better performance."""
    try:
        with open(REFERENCE_FILE_PATH, 'r') as f:
            reference_data = json.load(f)
        return reference_data
    except Exception as e:
        print(f"Error loading reference data: {e}")
        return {
            "soft_skills": [],
            "seniority_level": [],
            "location": [],
            "major": []
        }

def save_reference_data(reference_data):
    """Save updated reference data to JSON file."""
    try:
        with open(REFERENCE_FILE_PATH, 'w') as f:
            json.dump(reference_data, f, indent=2)
        print("Reference data updated successfully")
        load_reference_data.cache_clear()
    except Exception as e:
        print(f"Error saving reference data: {e}")

def preprocess_text(text):
    if not text or not isinstance(text, str):
        return ""
    # Convert to lowercase and remove special characters
    return re.sub(r'[^\w\s]', '', text.lower()).strip()

def create_normalize_location_udf(normalizer):
    """Create a UDF for location normalization that can be used with Polars"""
    def normalize_location(loc):
        # Handle null/None values
        if loc is None:
            return None
            
        # Ensure we're working with strings
        if not isinstance(loc, str):
            try:
                loc = str(loc).strip()
            except:
                return None
                
        if not loc:
            return None
            
        norm_loc, _ = normalizer.normalize(loc)
        return norm_loc
    return normalize_location

def create_normalize_list_udf(normalizer):
    """Create a UDF for list normalization (skills, majors, tech_stack) that can be used with Polars"""
    def normalize_list(items):
        # Handle null/None values
        if items is None:
            return []
            
        # If we get a string instead of a list, try to parse it as a list
        if isinstance(items, str):
            try:
                # Check if it's a string representation of a list
                if items.strip().startswith('[') and items.strip().endswith(']'):
                    import json
                    items = json.loads(items)
                else:
                    # Single item
                    items = [items]
            except:
                return []
                
        # Ensure it's a list
        if not isinstance(items, list):
            try:
                items = list(items)
            except:
                return []
        
        normalized_items = []
        for item in items:
            # Skip None or empty items
            if not item:
                continue
                
            # Ensure item is a string
            if not isinstance(item, str):
                try:
                    item = str(item).strip()
                except:
                    continue
                    
            if not item:
                continue
                
            try:
                norm_result = normalizer.normalize(item)
                if norm_result and isinstance(norm_result, tuple) and len(norm_result) == 2:
                    norm_item, _ = norm_result
                    if norm_item:
                        normalized_items.append(norm_item)
            except Exception as e:
                # Log the error but continue processing
                print(f"Error normalizing item '{item}': {str(e)}")
                continue
                
        return normalized_items
    return normalize_list


class ReferenceNormalizer:
    """Class to handle normalization with efficient preprocessing and caching"""
    
    def __init__(self, category, threshold=50):
        """Initialize normalizer for a specific category (location, major, soft_skills, tech_stack)"""
        self.category = category
        self.threshold = threshold
        self.reference_data = None
        self.processed_mapping = None
        self.new_values = set()
        self.cache = {}  # Simple in-memory cache for normalized values
        self.load_data()
    
    def load_data(self):
        """Load and preprocess reference data"""
        all_data = load_reference_data()
        self.reference_data = all_data.get(self.category, [])
        # Create mapping once during initialization
        self.processed_mapping = {preprocess_text(ref): ref for ref in self.reference_data}
    
    def normalize(self, value):
        """Normalize a single value using efficient processing"""
        if not value or not isinstance(value, str):
            return None, False
            
        value = value.strip()
        if not value:
            return None, False
            
        # Check cache first for exact matches
        if value in self.cache:
            return self.cache[value], False
            
        # If exact match exists in original data
        if value in self.reference_data:
            self.cache[value] = value
            return value, False
            
        # Preprocess the value
        processed_value = preprocess_text(value)
        
        # Check for preprocessed exact matches
        if processed_value in self.processed_mapping:
            normalized = self.processed_mapping[processed_value]
            self.cache[value] = normalized
            return normalized, False
        
        # Use fuzzy matching only when necessary
        try:
            from fuzzywuzzy import fuzz, process
            
            # Find best match using preprocessed values for efficiency
            best_match, score = process.extractOne(
                processed_value, 
                list(self.processed_mapping.keys()), 
                scorer=fuzz.token_set_ratio
            )
            
            if score >= self.threshold:
                normalized = self.processed_mapping[best_match]
                self.cache[value] = normalized
                return normalized, False
        except ImportError:
            pass
            
        # If no good match, mark as new
        self.new_values.add(value)
        self.cache[value] = value
        return value, True
    
    def update_reference_if_needed(self):
        """Update reference data if new values were found"""
        if not self.new_values:
            return False
            
        # Add new values to reference data
        reference_data = load_reference_data()
        category_data = set(reference_data.get(self.category, []))
        category_data.update(self.new_values)
        reference_data[self.category] = sorted(list(category_data))
        
        # Save updated reference data
        save_reference_data(reference_data)
        
        # Reset for next batch
        self.new_values = set()
        self.load_data()  # Reload data with new values
        return True


@asset(
    name="silver_normalized_location",
    description="Normalize location data from bronze layer",
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "cv_assistant"],
    compute_kind="Normalization",
    group_name="silver",
    partitions_def=MONTHLY,
    ins={
        "bronze_job_descriptions": AssetIn(key=["bronze", "cv_assistant", "bronze_job_descriptions"]),
    },
)
def normalize_location(context, bronze_job_descriptions):
    """
    Asset to normalize location in job descriptions with improved performance using Polars.
    Location data is expected to be a string.
    """
    normalizer = ReferenceNormalizer('location')
    
    # Use Polars DataFrame directly
    df = bronze_job_descriptions
    
    if "location" not in df.columns:
        context.log.warning("Location column not found in input data")
        return Output(
            df, 
            metadata={
                "normalized_field": "location",
                "row_count": df.shape[0],
                "partition": context.asset_partition_key_for_output(),
                "status": "no_location_column"
            }
        )
    
    # Create a Python UDF for normalization
    normalize_loc_udf = create_normalize_location_udf(normalizer)
    
    # Use lazy evaluation for better performance
    df_lazy = df.lazy()
    df_lazy = df_lazy.with_columns(
        pl.col("location").map_elements(normalize_loc_udf).alias("normalized_location")
    )
    # Drop original column to save memory, keep only normalized results if needed
    if "location" in df.columns:
        df_lazy = df_lazy.drop("location") 
    # Only collect when the result is needed
    df_with_norm = df_lazy.collect()
    
    # Update reference data with any new values found
    updated = normalizer.update_reference_if_needed()
    
    return Output(
        df_with_norm,
        metadata={
            "normalized_field": "location",
            "row_count": df.shape[0],
            "reference_updated": updated,
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )


@asset(
    name="silver_normalized_major",
    description="Normalize major data from bronze layer",
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "cv_assistant"],
    compute_kind="Normalization",
    group_name="silver",
    partitions_def=MONTHLY,
    ins={
        "bronze_job_descriptions": AssetIn(key=["bronze", "cv_assistant", "bronze_job_descriptions"]),
    },
)
def normalize_major(context, bronze_job_descriptions):
    """
    Asset to normalize major in job descriptions with improved performance using Polars.
    Major data is expected to be a list of strings.
    """
    normalizer = ReferenceNormalizer('major')
    
    # Use Polars DataFrame directly
    df = bronze_job_descriptions
    
    if "major" not in df.columns:
        context.log.warning("Required majors column not found in input data")
        return Output(
            df,
            metadata={
                "normalized_field": "major",
                "row_count": df.shape[0],
                "partition": context.asset_partition_key_for_output(),
                "status": "no_majors_column"
            }
        )
    
    # Create a Python UDF for normalization
    normalize_majors_udf = create_normalize_list_udf(normalizer)
    
    # Sử dụng lazy evaluation để tối ưu hiệu năng
    df_lazy = df.lazy()
    df_lazy = df_lazy.with_columns(
        pl.col("major").map_elements(normalize_majors_udf).alias("normalized_major")
    )
    # Xóa cột gốc để tiết kiệm bộ nhớ
    if "major" in df.columns:
        df_lazy = df_lazy.drop("major")
    
    # Collect chỉ khi cần kết quả
    df_with_norm = df_lazy.collect()
    
    # Update reference data with any new values found
    updated = normalizer.update_reference_if_needed()
    
    return Output(
        df_with_norm,
        metadata={
            "normalized_field": "major",
            "row_count": df.shape[0],
            "reference_updated": updated,
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )


@asset(
    name="silver_normalized_soft_skills",
    description="Normalize soft skills data from bronze layer",
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "cv_assistant"],
    compute_kind="Normalization",
    group_name="silver",
    partitions_def=MONTHLY,
    ins={
        "bronze_job_descriptions": AssetIn(key=["bronze", "cv_assistant", "bronze_job_descriptions"]),
    },
)
def normalize_soft_skills(context, bronze_job_descriptions):
    """
    Asset to normalize soft skills in job descriptions with improved performance using Polars.
    Soft skills data is expected to be a list of strings.
    """
    normalizer = ReferenceNormalizer('soft_skills')
    
    # Use Polars DataFrame directly
    df = bronze_job_descriptions
    
    if "soft_skills" not in df.columns:
        context.log.warning("Skills column not found in input data")
        return Output(
            df,
            metadata={
                "normalized_field": "soft_skills",
                "row_count": df.shape[0],
                "partition": context.asset_partition_key_for_output(),
                "status": "no_skills_column"
            }
        )
    
    # Create a Python UDF for normalization
    normalize_skills_udf = create_normalize_list_udf(normalizer)
    
    # Sử dụng lazy evaluation để tối ưu hiệu năng
    df_lazy = df.lazy()
    df_lazy = df_lazy.with_columns(
        pl.col("soft_skills").map_elements(normalize_skills_udf).alias("normalized_soft_skills")
    )
    # Xóa cột gốc để tiết kiệm bộ nhớ nếu cần
    if "soft_skills" in df.columns:
        df_lazy = df_lazy.drop("soft_skills")
    
    # Collect chỉ khi cần kết quả
    df_with_norm = df_lazy.collect()
    
    # Update reference data with any new values found
    updated = normalizer.update_reference_if_needed()
    
    return Output(
        df_with_norm,
        metadata={
            "normalized_field": "soft_skills",
            "row_count": df.shape[0],
            "reference_updated": updated,
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )

@asset(
    name="silver_normalized_tech_stack",
    description="Normalize tech stack data from bronze layer",
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "cv_assistant"],
    compute_kind="Normalization",
    group_name="silver",
    partitions_def=MONTHLY,
    ins={
        "bronze_job_descriptions": AssetIn(key=["bronze", "cv_assistant", "bronze_job_descriptions"]),
    },
)
def normalize_tech_stack(context, bronze_job_descriptions):
    """
    Asset to normalize tech stack in job descriptions with improved performance using Polars.
    Tech stack data is expected to be a list of strings.
    """
    normalizer = ReferenceNormalizer('tech_stack')
    
    # Use Polars DataFrame directly
    df = bronze_job_descriptions
    
    if "tech_stack" not in df.columns:
        context.log.warning("Tech stack column not found in input data")
        return Output(
            df,
            metadata={
                "normalized_field": "tech_stack",
                "row_count": df.shape[0],
                "partition": context.asset_partition_key_for_output(),
                "status": "no_tech_stack_column"
            }
        )
    
    # Create a Python UDF for normalization
    normalize_tech_stack_udf = create_normalize_list_udf(normalizer)
    
    # Sử dụng lazy evaluation để tối ưu hiệu năng
    df_lazy = df.lazy()
    df_lazy = df_lazy.with_columns(
        pl.col("tech_stack").map_elements(normalize_tech_stack_udf).alias("normalized_tech_stack")
    )
    # Xóa cột gốc để tiết kiệm bộ nhớ nếu cần
    if "tech_stack" in df.columns:
        df_lazy = df_lazy.drop("tech_stack")
    
    # Collect chỉ khi cần kết quả
    df_with_norm = df_lazy.collect()
    
    # Update reference data with any new values found
    updated = normalizer.update_reference_if_needed()
    
    return Output(
        df_with_norm,
        metadata={
            "normalized_field": "tech_stack",
            "row_count": df.shape[0],
            "reference_updated": updated,
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )

@asset(
    name="silver_job_descriptions",
    description="Combine all normalized data into a silver layer dataframe",
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "cv_assistant"],
    compute_kind="Combination",
    group_name="silver",
    partitions_def=MONTHLY,
    ins={
        "silver_normalized_location": AssetIn(key=["silver", "cv_assistant", "silver_normalized_location"]),
        "silver_normalized_major": AssetIn(key=["silver", "cv_assistant", "silver_normalized_major"]),
        "silver_normalized_soft_skills": AssetIn(key=["silver", "cv_assistant", "silver_normalized_soft_skills"]),
        "silver_normalized_tech_stack": AssetIn(key=["silver", "cv_assistant", "silver_normalized_tech_stack"]),
    },
)
def silver_job_descriptions(context, silver_normalized_location, silver_normalized_major, silver_normalized_soft_skills, silver_normalized_tech_stack):
    """
    Combine all normalized data into a silver layer dataframe using Polars.
    """
    join_key = "_id"
    result_df = pl.DataFrame()
    if "normalized_soft_skills" in silver_normalized_soft_skills.columns:
        result_df = silver_normalized_soft_skills

    if "normalized_location" in silver_normalized_location.columns:
        result_df = result_df.join(
            silver_normalized_location.select([join_key, "normalized_location"]), 
            on=join_key,
            how="left"
        )
    
    if "normalized_major" in silver_normalized_major.columns:
        result_df = result_df.join(
            silver_normalized_major.select([join_key, "normalized_major"]), 
            on=join_key,
            how="left"
        )
    
    if "normalized_tech_stack" in silver_normalized_tech_stack.columns:
        result_df = result_df.join(
            silver_normalized_tech_stack.select([join_key, "normalized_tech_stack"]), 
            on=join_key,
            how="left"
        )

    if result_df.is_empty():
        context.log.info("No data to combine in silver layer")
        return Output(
            pl.DataFrame(),  # Empty DataFrame
            metadata={
                "row_count": 0,
                "partition": context.asset_partition_key_for_output(),
                "status": "empty"
            }
        )
    
    for col in result_df.columns:
        if col.startswith("normalized_"):
            original_col = col.replace("normalized_", "")
            if original_col in result_df.columns:
                result_df = result_df.drop(original_col)
            result_df = result_df.rename({col: original_col})
    
    return Output(
        result_df,
        metadata={
            "row_count": result_df.shape[0],
            "column_count": len(result_df.columns),
            "normalized_columns": str([col for col in result_df.columns if col.startswith("normalized_")]),
            "partition": context.asset_partition_key_for_output(),
            "status": "success"
        }
    )