import logging
import sys
from pathlib import Path
import importlib.util
import os

from dagster import asset, Output, ScheduleDefinition

# Setup path to access backend config
backend_path = Path(__file__).resolve().parents[3]  # Go up 3 levels to reach backend
if str(backend_path) not in sys.path:
    sys.path.insert(0, str(backend_path))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@asset(
    name="run_jd_parser",
    description="Run the test_mongo_model.py script to parse job descriptions and store in MongoDB",
    compute_kind="MongoDB",
    group_name="parser",
    # Define a default schedule to run daily at midnight
    freshness_policy={"maximum_lag_minutes": 24 * 60}  # 24 hours
)
def run_jd_parser(context) -> Output[dict]:
    """Asset that runs the test_mongo_model.py script directly"""
    context.log.info("Starting to run the MongoDB job description parser script")
    
    # Path to the test_mongo_model.py script
    script_path = Path(__file__).resolve().parent.parent / "ai_models" / "test_mongo_model.py"
    
    if not script_path.exists():
        error_msg = f"Script file not found at {script_path}"
        context.log.error(error_msg)
        return Output(
            {"status": "failed", "error": error_msg},
            metadata={
                "status": "failed",
                "error": error_msg
            }
        )
    
    context.log.info(f"Found script at {script_path}")
    
    # Run the script
    try:
        # Setup to capture stdout/stderr
        from io import StringIO
        import contextlib
        import re
        
        output_capture = StringIO()
        error_capture = StringIO()
        
        with contextlib.redirect_stdout(output_capture):
            with contextlib.redirect_stderr(error_capture):
                # Load the script as a module and execute it
                spec = importlib.util.spec_from_file_location("test_mongo_model", script_path)
                module = importlib.util.module_from_spec(spec)
                sys.modules["test_mongo_model"] = module
                spec.loader.exec_module(module)
        
        # Get the captured output
        output_text = output_capture.getvalue()
        error_text = error_capture.getvalue()
        
        # Process the results
        results = {
            "status": "completed",
            "output": output_text,
            "errors": error_text,
            "processed_count": 0
        }
        
        # Extract the number of processed job descriptions
        processed_match = re.search(r"Đã xử lý thành công (\d+) mô tả công việc", output_text)
        if processed_match:
            results["processed_count"] = int(processed_match.group(1))
            
        if error_text.strip():
            results["status"] = "completed_with_errors"
        
        context.log.info(f"MongoDB parser script completed with status: {results['status']}")
        if results["processed_count"] > 0:
            context.log.info(f"Successfully processed {results['processed_count']} job descriptions")
        
        return Output(
            results,
            metadata={
                "status": results["status"],
                "processed_count": results["processed_count"],
                "error_sample": error_text[:200] + "..." if len(error_text) > 200 else error_text,
                "output_sample": output_text[:200] + "..." if len(output_text) > 200 else output_text
            }
        )
        
    except Exception as e:
        error_msg = f"Error running MongoDB parser script: {str(e)}"
        context.log.error(error_msg)
        
        return Output(
            {"status": "failed", "error": str(e)},
            metadata={
                "status": "failed",
                "error": str(e)
            }
        )

# Create a schedule definition for daily execution
jd_parser_schedule = ScheduleDefinition(
    name="daily_jd_parser",
    cron_schedule="0 0 * * *",  # Run daily at midnight
    asset_selection=["run_jd_parser"],
    description="Schedule to run the JD parser script daily"
)

defs = {
    "schedules": [jd_parser_schedule]
}