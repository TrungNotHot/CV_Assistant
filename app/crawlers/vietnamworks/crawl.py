import json
import time
import requests
from bs4 import BeautifulSoup
from config import BASE_API_URL, BASE_SITE_URL, HEADERS, JOB_FUNCTION_FILTER, JOB_DESCRIPTION_SELECTOR, HITS_PER_PAGE, REQUEST_DELAY

def crawl_jobs_from_api(page):
    payload = {
        "userId": 0,
        "query": "",
        "filter": [{"field": "jobFunction", "value": json.dumps(JOB_FUNCTION_FILTER)}],
        "ranges": [],
        "order": [],
        "hitsPerPage": HITS_PER_PAGE,
        "page": page,
        "retrieveFields": ["jobTitle", "companyName", "workingLocations", "jobUrl", "alias", "jobId"],
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
    jobs = []
    for job in api_data.get("data", []):
        job_url = job.get("jobUrl") or f"{BASE_SITE_URL}/{job['alias']}{job['jobId']}-jv"
        if not job_url.startswith("http"):
            continue

        location = job.get("workingLocations", [{}])[0].get("cityNameVI", "Unknown")
        full_text = get_full_text_jd_from_url(job_url)
        if full_text.strip():
            jobs.append({
                "job_title": job.get("jobTitle"),
                "company": job.get("companyName"),
                "location": location,
                "full_text_jd": full_text
            })
            print(f"✅ Parsed: {job.get('jobTitle')} - {job.get('companyName')}")
        time.sleep(REQUEST_DELAY)
    return jobs


def crawl_all_pages():
    print("📥 Getting total page count...")
    first_page = crawl_jobs_from_api(0)
    total_pages = first_page["meta"]["nbPages"]
    print(f"📊 Total pages: {total_pages}")

    all_jobs = parse_jobs(first_page)
    for page in range(1, total_pages):
        print(f"📦 Crawling page {page + 1}/{total_pages}")
        try:
            data = crawl_jobs_from_api(page)
            all_jobs += parse_jobs(data)
        except Exception as e:
            print(f"❌ Error on page {page}: {e}")
    return all_jobs


if __name__ == "__main__":
    jobs = crawl_all_pages()
    with open("vietnamworks_all_jobs.json", "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    print(f"\n🎯 Done. Extracted {len(jobs)} jobs → vietnamworks_all_jobs.json")
