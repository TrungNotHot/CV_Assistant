import os
import json
import polars as pl
import re
from dagster import asset

# Path to the reference file
REFERENCE_FILE_PATH = os.path.join(os.path.dirname(__file__), "../resources/reference.json")

def load_reference_data():
    """Load reference data from JSON file."""
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
    except Exception as e:
        print(f"Error saving reference data: {e}")

def normalize_value(value, reference_list, threshold=80):
    """
    Normalize a value using fuzzy matching against reference list.
    """
    if not value or not isinstance(value, str):
        return None, False
        
    # Clean the value
    value = value.strip()
    if not value:
        return None, False
        
    # If exact match exists, return it
    if value in reference_list:
        return value, False
    
    # Preprocess text for comparison
    def preprocess_text(text):
        if not text:
            return ""
        return re.sub(r'[^\w\s]', '', text.lower())
    
    processed_value = preprocess_text(value)
    processed_to_original = {preprocess_text(ref): ref for ref in reference_list}
    
    if processed_value in processed_to_original:
        return processed_to_original[processed_value], False
        
    if reference_list:
        try:
            from fuzzywuzzy import fuzz, process
            
            processed_refs = list(processed_to_original.keys())
            best_match, score = process.extractOne(processed_value, processed_refs, scorer=fuzz.token_sort_ratio)
            
            if score >= threshold:
                return processed_to_original[best_match], False
        except ImportError:
            for proc_ref, orig_ref in processed_to_original.items():
                if processed_value == proc_ref:
                    return orig_ref, False
            
    return value, True

@asset
def normalize_soft_skills(bronze_jobs_df):
    """Asset to normalize soft skills in job descriptions."""
    reference_data = load_reference_data()
    reference_soft_skills = reference_data["soft_skills"]
    
    # Convert pandas DataFrame to polars if needed
    if not isinstance(bronze_jobs_df, pl.DataFrame):
        df = pl.from_pandas(bronze_jobs_df)
    else:
        df = bronze_jobs_df.clone()
        
    if "skills" not in df.columns:
        return df
    
    new_soft_skills = set()
    
    def normalize_skills(skills_list):
        if not isinstance(skills_list, list):
            return []
            
        normalized_skills = []
        for skill in skills_list:
            norm_skill, is_new = normalize_value(skill, reference_soft_skills)
            if norm_skill:
                normalized_skills.append(norm_skill)
                if is_new:
                    new_soft_skills.add(norm_skill)
        return normalized_skills
    
    # Apply normalization with polars
    df = df.with_columns(
        pl.col("skills").map_elements(normalize_skills).alias("normalized_skills")
    )
    
    # Update reference data if new soft skills were found
    if new_soft_skills:
        reference_data["soft_skills"] = sorted(list(set(reference_soft_skills) | new_soft_skills))
        save_reference_data(reference_data)
        
    return df

@asset
def normalize_location(bronze_jobs_df):
    """Asset to normalize location in job descriptions."""
    reference_data = load_reference_data()
    reference_locations = reference_data["location"]
    
    # Convert pandas DataFrame to polars if needed
    if not isinstance(bronze_jobs_df, pl.DataFrame):
        df = pl.from_pandas(bronze_jobs_df)
    else:
        df = bronze_jobs_df.clone()
        
    if "location" not in df.columns:
        return df
    
    new_locations = set()
    
    def process_location(loc):
        if not loc:
            return None
        norm_loc, is_new = normalize_value(loc, reference_locations)
        if is_new and norm_loc:
            new_locations.add(norm_loc)
        return norm_loc
        
    df = df.with_columns(
        pl.col("location").map_elements(process_location).alias("normalized_location")
    )
    
    # Update reference data if new locations were found
    if new_locations:
        reference_data["location"] = sorted(list(set(reference_locations) | new_locations))
        save_reference_data(reference_data)
        
    return df

@asset
def normalize_major(bronze_jobs_df):
    """Asset to normalize major in job descriptions."""
    reference_data = load_reference_data()
    reference_majors = reference_data["major"]
    
    # Convert pandas DataFrame to polars if needed
    if not isinstance(bronze_jobs_df, pl.DataFrame):
        df = pl.from_pandas(bronze_jobs_df)
    else:
        df = bronze_jobs_df.clone()
        
    if "required_majors" not in df.columns:
        return df
    
    new_majors = set()
    
    def normalize_majors(majors_list):
        if not isinstance(majors_list, list):
            return []
            
        normalized_majors = []
        for major in majors_list:
            norm_major, is_new = normalize_value(major, reference_majors)
            if norm_major:
                normalized_majors.append(norm_major)
                if is_new:
                    new_majors.add(norm_major)
        return normalized_majors
    
    df = df.with_columns(
        pl.col("required_majors").map_elements(normalize_majors).alias("normalized_majors")
    )
    
    # Update reference data if new majors were found
    if new_majors:
        reference_data["major"] = sorted(list(set(reference_majors) | new_majors))
        save_reference_data(reference_data)
        
    return df

@asset
def silver_jobs_df(normalize_soft_skills, normalize_location, normalize_major):
    """Combine all normalized data into a silver layer dataframe."""
    # Start with the base dataframe
    df = normalize_soft_skills
    
    # Add normalized columns from other assets
    if "normalized_location" in normalize_location.columns:
        df = df.with_columns(normalize_location.select("normalized_location"))
    
    if "normalized_majors" in normalize_major.columns:
        df = df.with_columns(normalize_major.select("normalized_majors"))
        
    return df
