from Parser import JDParser
from ParserModel import JDParserModel
import os
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
# Create configuration dictionary with defaults
config = {
    "GEMINI_MODEL_NAME": os.getenv("GEMINI_MODEL_NAME"),
    "GEMINI_TOKEN": os.getenv("GEMINI_TOKEN"),
    "JD_JSON_FORMAT": str(os.getenv("JD_JSON_FORMAT")),
    "BATCH_SIZE": int(os.getenv("BATCH_SIZE")),
}

if __name__ == "__main__":

    data = {
            "job_title": "Performance QA Engineer (QA QC/Tester/English)",
            "company_name": "Nakivo",
            "location": "Ho Chi Minh",
            "posted_date": "2025-05-06",
            "full_text_jd": "Job description:\nJob description\n● Design, develop, and execute comprehensive performance testing strategies, including load, stress, scalability, and endurance tests\n● Analyze performance test results, identify bottlenecks, and provide actionable insights for optimization. Create detailed performance test reports for stakeholders\n● Work closely with development, and product management teams to understand product architecture, performance requirements, and business use cases\n● Develop and maintain automated performance testing scripts using industry-standard tools to streamline testing processes.\n● Utilize monitoring tools to track system performance and diagnose issues during testing\n● Contribute to the enhancement of performance testing methodologies, tools, and processes for improved efficiency and accuracy\n● Plan, configure, and maintain test environments, ensuring accurate simulation of production-like conditions\n● Investigate and troubleshoot performance issues, collaborating with development teams to identify and address root causes.\n● Create and maintain performance test documentation, including test plans, test scripts, and test results\n\nYour skills and experience:\nYour skills and experience\n● Bachelor's degree in Computer Science, Engineering, or a related field, or equivalent practical experience\n● 3-5 years of experience in performance testing, preferably in a software product environment\n● Strong experience with performance testing tools\n● Proficiency in scripting languages like Python, JavaScript, or Shell for automation purposes\n● Experience working with cloud platforms (AWS, Azure) and virtualized environments\n● Solid understanding of software development life cycle (SDLC) and Agile methodologies\n● Strong analytical and problem-solving skills with attention to detail\n● Excellent communication and teamwork abilities\nNice to have:\n● Experience with cloud platforms (AWS, Azure, Google Cloud) and virtual environments (VMware, Hyper-V, KVM)\n● Familiarity with backup and disaster recovery products like Nakivo, Veeam, Acronis, or similar\n● Ability to analyze performance metrics, diagnose issues, and suggest optimizations specific to data backup and recovery\n● Strong communication abilities to document and present performance test results\nGiven the high volume of CVs applied, we can only manage to respond to candidates whose profiles aligns with the position within 5 working days. We appreciate your understanding and enthusiastic towards our company",
            "hash": "30b6481f2617bee03370cdd1253ec84c",
            "source": "itviec",
            "source_id": "1643",
            "url": "https://itviec.com/it-jobs/performance-qa-engineer-qa-qc-tester-english-nakivo-1643",
            "update_date": "2025-05-06T14:14:14.038000"
    }
    jd_info_dict = JDParser.parse_jd(data, config=config)
    print(jd_info_dict)