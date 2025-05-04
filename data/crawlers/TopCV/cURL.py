from bs4 import BeautifulSoup
from pymongo import MongoClient
from browser_manager import BrowserManager
import json
import re
from datetime import datetime

class URLJob:
    def __init__(self, url):
        self.url = url
        self.browser = BrowserManager()
        self.title = ""
        self.date_post = None
        self.company_name = ""
        self.company_field = ""
        self.company_scope = ""
        self.experience = ""
        self.requirements = ""

    def fetch_data(self):
        page_source = self.browser.fetch_page(self.url)
        soup = BeautifulSoup(page_source, "html.parser")

        # Title
        title_tag = soup.find("title")
        full_title = title_tag.text.strip() if title_tag else ""
        self.title = full_title.split("làm việc tại")[0].strip() if "làm việc tại" in full_title else full_title

        # Date Posted
        script_tag = soup.find("script", {"type": "application/ld+json"})
        if script_tag:
            try:
                job_json = json.loads(script_tag.string)
                if isinstance(job_json, dict) and "datePosted" in job_json:
                    date_str = job_json["datePosted"]  # ví dụ "2025-04-25"
                    self.date_post = datetime.strptime(date_str, "%Y-%m-%d")
            except Exception as e:
                pass

        # Company name
        company_tag = soup.find("div", class_="company-name")
        self.company_name = company_tag.text.strip() if company_tag else ""

        # Company field
        field_div = soup.find("div", class_="job-detail__company--information-item company-field")
        if field_div:
            value = field_div.find("div", class_="company-value")
            self.company_field = value.text.strip() if value else ""

        # Company scope
        scope_div = soup.find("div", class_="job-detail__company--information-item company-scale")
        if scope_div:
            value = scope_div.find("div", class_="company-value")
            self.company_scope = value.text.strip() if value else ""

        # Experience
        for section in soup.find_all("div", class_="job-detail__info--section-content"):
            title_div = section.find("div", class_="job-detail__info--section-content-title")
            if title_div and "kinh nghiệm" in title_div.text.lower():
                value_div = section.find("div", class_="job-detail__info--section-content-value")
                self.experience = value_div.text.strip() if value_div else ""
                break

        # Yêu cầu ứng viên
        for section in soup.find_all("div", class_="job-description__item"):
            heading = section.find("h3")
            if heading and "yêu cầu ứng viên" in heading.text.lower():
                requirements_html = section.decode_contents().strip()
                self.requirements = self.clean_html_with_regex(requirements_html)
                break
    
    @staticmethod
    def clean_html_with_regex(html_str):

        if not html_str:
            return ""
        cleaned = re.sub(r'<[^>]+>', '\n', html_str)

        cleaned = re.sub(r'\n+', '\n', cleaned)
        cleaned = cleaned.strip()

        return cleaned


    def to_dict(self):
        return {
            "url": self.url,
            "title": self.title,
            "date_post": self.date_post.strftime("%Y-%m-%d") if self.date_post else "",
            "company_name": self.company_name,
            "company_field": self.company_field,
            "company_scope": self.company_scope,
            "experience": self.experience,
            "requirements": self.requirements
        }

    def push_to_mongo(self, mongo_uri="mongodb://localhost:27017/", db_name="topcv", collection_name="jobs"):
        try:
            client = MongoClient(mongo_uri)
            db = client[db_name]
            collection = db[collection_name]
            result = collection.insert_one(self.to_dict())
            print(f"[✓] Inserted job to MongoDB with _id: {result.inserted_id}")
        except Exception as e:
            print(f"[✗] MongoDB insert error: {e}")

    def save_to_json(self, filepath="job_data.jsonl", append=True):
        mode = "a" if append else "w"
        try:
            with open(filepath, mode, encoding="utf-8") as f:
                f.write(json.dumps(self.to_dict(), ensure_ascii=False) + "\n")
            print(f"[✓] Saved to {filepath}")
        except Exception as e:
            print(f"[✗] Failed to save JSON: {e}")
