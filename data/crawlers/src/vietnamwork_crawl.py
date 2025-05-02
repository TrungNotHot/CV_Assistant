import json
import time
import requests
import hashlib
from bs4 import BeautifulSoup
import argparse
from mongo_config import collection

BASE_API_URL = "https://ms.vietnamworks.com/job-search/v1.0/search"
BASE_SITE_URL = "https://www.vietnamworks.com"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/vnd.api+json",
    "Origin": BASE_SITE_URL,
    "Referer": BASE_SITE_URL,
    "User-Agent": "Mozilla/5.0"
}

JOB_FUNCTION_FILTER = [{"parentId": 5, "childrenIds": [-1]}]
JOB_DESCRIPTION_SELECTOR = "div.sc-1671001a-6.dVvinc"
HITS_PER_PAGE = 50
REQUEST_DELAY = 1.2

def crawl_jobs_from_api(page):
    payload = {
        "userId": 0,
        "query": "",
        "filter": [{"field": "jobFunction", "value": json.dumps(JOB_FUNCTION_FILTER)}],
        "ranges": [],
        "order": [],
        "hitsPerPage": HITS_PER_PAGE,
        "page": page,
        "retrieveFields": [
            "jobTitle", "companyName", "workingLocations", "jobUrl", "alias",
            "jobId", "approvedOn"
        ],
        "summaryVersion": ""
    }
    response = requests.post(BASE_API_URL, headers=HEADERS, json=payload)
    response.raise_for_status()
    return response.json()

def get_full_text_jd_from_url(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        jd_blocks = soup.select(JOB_DESCRIPTION_SELECTOR)
        return "\n\n".join(block.get_text(separator="\n", strip=True) for block in jd_blocks) if jd_blocks else ""
    except Exception as e:
        print(f"❌ Failed to fetch JD from {url}: {e}")
        return ""

def parse_jobs(api_data):
    inserted = 0
    for job in api_data.get("data", []):
        job_title = job.get("jobTitle")
        company_name = job.get("companyName")
        job_url = job.get("jobUrl") or f"{BASE_SITE_URL}/{job['alias']}{job['jobId']}-jv"
        posted_date = job.get("approvedOn", "")[:10]
        source_id = str(job.get("jobId"))
        location = job.get("workingLocations", [{}])[0].get("cityNameVI", "Unknown")

        if not job_url.startswith("http"):
            continue

        full_text = get_full_text_jd_from_url(job_url)
        if not full_text.strip():
            continue

        key = f"{job_title}{company_name}{posted_date}"
        hashed = hashlib.md5(key.encode("utf-8")).hexdigest()

        job_data = {
            "job_title": job_title,
            "company_name": company_name,
            "hash": hashed,
            "location": location,
            "posted_date": posted_date,
            "source": "vietnamwork",
            "source_id": source_id,
            "url": job_url,
            "full_text_jd": full_text
        }

        result = collection.update_one({"hash": hashed}, {"$setOnInsert": job_data}, upsert=True)
        if result.upserted_id:
            inserted += 1
            print(f"✅ Inserted: {job_title} - {company_name}")
        else:
            print(f"⚠️ Skipped duplicate: {job_title} - {company_name}")

        time.sleep(REQUEST_DELAY)
    return inserted

def crawl_all_pages(start_page=0):
    print("📥 Getting total page count...")
    first_page = crawl_jobs_from_api(0)
    total_pages = first_page["meta"]["nbPages"]
    print(f"📊 Total pages: {total_pages}")

    total_inserted = 0

    if start_page == 0:
        print(f"📄 Crawling page 1")
        total_inserted += parse_jobs(first_page)
        page_range = range(1, total_pages)
    else:
        page_range = range(start_page, total_pages)

    for page in page_range:
        print(f"📄 Crawling page {page + 1}")
        try:
            data = crawl_jobs_from_api(page)
            total_inserted += parse_jobs(data)
        except Exception as e:
            print(f"❌ Error on page {page + 1}: {e}")

    print(f"\n🎯 Total inserted into MongoDB: {total_inserted}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-page", type=int, default=1, help="Page number to start crawling from (1-based)")
    args = parser.parse_args()

    api_start_page = max(0, args.start_page - 1)
    crawl_all_pages(start_page=api_start_page)

