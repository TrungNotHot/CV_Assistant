from .CVParserModel import CVParserModel
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage

class CVParserGPT(CVParserModel):
    def __init__(self, model_name, token, cv_format):
        self.model_name = model_name
        self.cv_format = cv_format
        # Initialize LangChain's ChatOpenAI model
        self.client = ChatOpenAI(
            model_name=model_name,
            openai_api_key=token
        )
    
    def query(self, cv_text):
        # Construct the extraction message
        cv_extraction_msg = (
            f'"{cv_text}"\n---\nExtract information from this CV into the following JSON format with utf-8 encoding:\n'
            f'{self.cv_format}'
        )
        
        # Create messages for LangChain
        messages = [
            SystemMessage(content="You are a CV extractor to extract CV information into a corresponding format."),
            HumanMessage(content=cv_extraction_msg)
        ]
        
        # Query using LangChain's invoke method
        response = self.client.invoke(messages)
        
        # Extract the content from the response
        return response.content