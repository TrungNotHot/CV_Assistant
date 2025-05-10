from abc import ABC, abstractmethod
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables.config import RunnableConfig
from typing import List, Dict, Any, Optional
import logging


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
        # For Gemini model, if anothor model, parameter inside may be different
        return model_call(
            model=model_name,
            google_api_key=token
        )
    
    def query(self, jd_text):
        # Construct the extraction message
        jd_extraction_msg = (
            f'"{jd_text}"\n---\nExtract information in English from JD into the following JSON format with utf-8 encoding:\n'
            f'{self.parse_format}'
        )
        
        # Create messages for LangChain
        messages = [
            SystemMessage(content="You are a job description extractor to extract job information into a corresponding format."),
            HumanMessage(content=jd_extraction_msg)
        ]
        
        # Query using LangChain's invoke method
        response = self.client.invoke(messages)
        
        # Extract the content from the response
        return response.content
    
    def batch_query(self, texts_list: List[str], batch_size: int = 10) -> List[str]:
        """Process multiple JD texts in batches using LangChain's batch capability"""
        # Create system message once to reuse
        system_message = SystemMessage(content="You are a job description extractor to extract job information into a corresponding format.")
        
        # Prepare all inputs
        all_messages = []
        for jd_text in texts_list:
            jd_extraction_msg = (
                f'"{jd_text}"\n---\nExtract information in English from JD into the following JSON format with utf-8 encoding:\n'
                f'{self.parse_format}'
            )
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
        

# class CVParserModel(BaseParserModel):
#     def initialize_model(self, model_call, model_name, token):
#         # For Gemini model, if anothor model, parameter inside may be different
#         return model_call(
#             model_name=model_name,
#             openai_api_key=token
#         )
    
#     def query(self, cv_text):
#         # Construct the extraction message
#         cv_extraction_msg = (
#             f'"{cv_text}"\n---\nExtract information in English from CV into the following JSON format with utf-8 encoding:\n'
#             f'{self.parse_format}'
#         )
        
#         # Create messages for LangChain
#         messages = [
#             SystemMessage(content="You are a CV extractor to extract CV information into a corresponding format."),
#             HumanMessage(content=cv_extraction_msg)
#         ]
        
#         # Query using LangChain's invoke method
#         response = self.client.invoke(messages)
        
#         # Extract the content from the response
#         return response.content
    
#     def batch_query(self, texts_list: List[str], batch_size: int = 10) -> List[str]:
#         """Process multiple CV texts in batches using LangChain's batch capability"""
#         # Create system message once to reuse
#         system_message = SystemMessage(content="You are a CV extractor to extract CV information into a corresponding format.")
        
#         # Prepare all inputs
#         all_messages = []
#         for cv_text in texts_list:
#             cv_extraction_msg = (
#                 f'"{cv_text}"\n---\nExtract information in English from CV into the following JSON format with utf-8 encoding:\n'
#                 f'{self.parse_format}'
#             )
#             all_messages.append([
#                 system_message,
#                 HumanMessage(content=cv_extraction_msg)
#             ])
        
#         # Use LangChain's batch processing
#         config = RunnableConfig(max_concurrency=batch_size)
#         responses = self.client.batch(all_messages, config=config)
        
#         # Extract content from responses
#         results = [response.content for response in responses]
#         return results