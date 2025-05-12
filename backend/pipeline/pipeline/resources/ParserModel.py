from abc import ABC, abstractmethod
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables.config import RunnableConfig
from typing import List, Dict, Any, Optional
import logging
import json
import os


class BaseParserModel(ABC):
    def __init__(self, model_call, model_name, token, parse_format):
        self.model_name = model_name
        self.parse_format = parse_format
        # Initialize LangChain model
        self.client = self.initialize_model(model_call, model_name, token)
    
    @abstractmethod
    def initialize_model(self, model_call, model_name, token):
        pass

    @abstractmethod
    def query(self, full_text):
        pass
    
    @abstractmethod
    def batch_query(self, texts_list: List[str], batch_size: int = 10) -> List[str]:
        """
        Process multiple texts in batches using LangChain's built-in batching capabilities
        
        Args:
            texts_list: List of texts to process
            batch_size: Number of texts to process in each batch
            
        Returns:
            List of processed results
        """
        pass


class JDParserModel(BaseParserModel):
    def initialize_model(self, model_call, model_name, token):
        # Load reference data for better prompting
        self.reference_data = self._load_reference_data()
        
        # For Gemini model, if anothor model, parameter inside may be different
        return model_call(
            model=model_name,
            google_api_key=token
        )
    
    def _load_reference_data(self) -> Dict[str, List[str]]:
        """Load reference data for standardized extraction"""
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            reference_path = os.path.join(current_dir, "reference.json")
            
            with open(reference_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading reference data: {str(e)}")
            return {
                "tech_stack": [],
                "soft_skills": [],
                "seniority_level": [],
                "major": [],
                "job_position": []
            }
    
    def _create_enhanced_prompt(self, jd_text: str) -> str:
        """Create an enhanced prompt using reference data"""
        # Create tech stack list for context
        tech_stack_str = ", ".join(self.reference_data.get("tech_stack", []))
        # Create soft skills list for context
        soft_skills_str = ", ".join(self.reference_data.get("soft_skills", []))
        # Create seniority levels list for context
        seniority_str = ", ".join(self.reference_data.get("seniority_level", []))
        # Create major list for context
        major_str = ", ".join(self.reference_data.get("major", []))
        # Create job position list for context
        job_position_str = ", ".join(self.reference_data.get("job_position", []))
        
        # Construct the enhanced extraction message
        return (
            f'"{jd_text}"\n\n---\n\n'
            f'Extract key information from the job description above into this JSON format:\n'
            f'{self.parse_format}\n\n'
            f'Guidelines:\n'
            f'1. Respond only in English\n'
            f'2. For soft_skills: Match or standardize using format from these standard terms when possible: {soft_skills_str}\n'
            f'3. For seniority_level: Categorize as one or more of: {seniority_str}\n'
            f'4. For majors: Assign to one or more of: {major_str}\n'
            f'5. For job_position: Match or standardize using format from these standard terms when possible: {job_position_str}\n'
            f'6. For tech_stack: Normalize technical terms to their official names\n'
            f'   (e.g., "nodejs" → "Node.js", "postgres" → "PostgreSQL")\n'
            f'   Use these standard terms when applicable: {tech_stack_str}\n'
            f'7. Return valid JSON with proper formatting and ensure all fields are present\n'
        )
    
    def query(self, jd_text):
        # Construct the enhanced extraction message
        jd_extraction_msg = self._create_enhanced_prompt(jd_text)
        
        # Create messages for LangChain
        messages = [
            SystemMessage(content="You are a job description analyzer specialized in extracting structured information from job postings. Follow the requested JSON format exactly."),
            HumanMessage(content=jd_extraction_msg)
        ]
        
        # Query using LangChain's invoke method
        response = self.client.invoke(messages)
        
        # Extract the content from the response
        return response.content
    
    def batch_query(self, texts_list: List[str], batch_size: int = 10) -> List[str]:
        """Process multiple JD texts in batches using LangChain's batch capability"""
        # Create system message once to reuse
        system_message = SystemMessage(content="You are a job description analyzer specialized in extracting structured information from job postings. Follow the requested JSON format exactly.")
        
        # Prepare all inputs
        all_messages = []
        for jd_text in texts_list:
            jd_extraction_msg = self._create_enhanced_prompt(jd_text)
            all_messages.append([
                system_message,
                HumanMessage(content=jd_extraction_msg)
            ])
        
        # Use LangChain's batch processing with error handling
        try:
            config = RunnableConfig(max_concurrency=batch_size)
            responses = self.client.batch(all_messages, config=config)
            
            # Extract content from responses
            results = [response.content for response in responses]
            return results
        except Exception as e:
            logging.error(f"Error in batch processing: {str(e)}")
            # Fall back to sequential processing if batch fails
            results = []
            for messages in all_messages:
                try:
                    response = self.client.invoke(messages)
                    results.append(response.content)
                except Exception as inner_e:
                    logging.error(f"Error in fallback processing: {str(inner_e)}")
                    results.append(None)
            return results