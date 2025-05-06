# GPT Configuration
GPT_MODEL_NAME = 'gpt-3.5-turbo-0125'
GPT_TOKEN = ''


# Google Gemini Configuration
GEMINI_MODEL_NAME = 'gemini-2.0-flash-lite'
GEMINI_TOKEN = 'AIzaSyAJOirhubiBU5AtiVNiwYOAv3e1tSBh0q8'


# Format config
JOB_PAGE_SIZE = 5
CV_JSON_FORMAT = \
'''
{
    "Profession": "",
    "Name": "",
    "Date of Birth": "",
    "Phone": "",
    "Address": "",
    "Email": "",
    "Website": "",
    "Skills": [],
    "Experiences": [],
    "Education": [],
    "Certificates": [],
    "References": []
}
'''
JD_JSON_FORMAT = \
'''
{
    "salary": "",
    "seniority_level": "",
    "major": "",
    "benefits": [],
    "skills (in breaf)": [],
    "tech_stack": [],
}
'''


# MongoDB Configuration
# MONGODB_URI = "mongodb+srv://admin:admin123@grab-11-cluster.gf512j3.mongodb.net/?retryWrites=true&w=majority"
MONGODB_URI = "mongodb://admin:admin123@localhost:27017/grab_db?authSource=admin"
MONGODB_DATABASE = "cv_assistant"
MONGODB_JD_COLLECTION = "job_descriptions"
MONGODB_PARSED_JD_COLLECTION = "parsed_job_descriptions"


# Batch Processing Configuration
BATCH_SIZE = 20