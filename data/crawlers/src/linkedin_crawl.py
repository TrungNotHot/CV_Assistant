import argparse
import requests
from bs4 import BeautifulSoup
import hashlib
import time
import random
from mongo_config import collection
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BASE_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
FIXED_PARAMS = {
    "keywords": "IT",
    "location": "Vietnam",
    "geoId": "104195383",
    "f_TPR": "r2592000",  # Posted in last 30 days
    "position": 1,
    "pageNum": 0,
}

# --- Rotating headers ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.85 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml",
        "Referer": "https://www.linkedin.com/jobs/search/",
        "Connection": "keep-alive",
    }

def get_full_text_jd(job_url: str) -> str:
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    try:
        driver = uc.Chrome(options=options)
        driver.get(job_url)
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CLASS_NAME, "show-more-less-html__markup"))
        )
        content = driver.execute_script("""
            const elem = document.querySelector('.show-more-less-html__markup');
            if (elem) {
                elem.querySelectorAll("img, svg, i, picture").forEach(e => e.remove());
                return elem.innerText.trim();
            }
            return "";
        """)
        return content.replace("\n\n", "\n") if content else ""
    except Exception as e:
        print(f"❌ Error fetching JD from {job_url}: {e}")
        return ""
    finally:
        try:
            driver.quit()
        except:
            pass

def parse_jobs(html: str):
    soup = BeautifulSoup(html, "html.parser")
    job_cards = soup.select("li")
    inserted = 0

    for job in job_cards:
        try:
            job_title = job.select_one("h3.base-search-card__title").get_text(strip=True)
            company_name = job.select_one("h4.base-search-card__subtitle").get_text(strip=True)
            location = job.select_one("span.job-search-card__location").get_text(strip=True)
            posted_date = job.select_one("time")["datetime"]
            url = job.select_one("a.base-card__full-link")["href"]
            source_id = url.split("-")[-1].split("?")[0]

            full_text_jd = get_full_text_jd(url)
            if not full_text_jd.strip():
                print(f"⚠️ Empty JD: {url}")
                continue

            hash_key = f"{company_name}{job_title}{posted_date}"
            hashed = hashlib.md5(hash_key.encode("utf-8")).hexdigest()

            job_data = {
                "job_title": job_title,
                "company_name": company_name,
                "hash": hashed,
                "location": location,
                "posted_date": posted_date,
                "source": "linkedin",
                "source_id": source_id,
                "url": url,
                "full_text_jd": full_text_jd
            }

            result = collection.update_one({"hash": hashed}, {"$setOnInsert": job_data}, upsert=True)
            if result.upserted_id:
                inserted += 1
                print(f"✅ Inserted: {job_title} - {company_name}")
            else:
                print(f"⚠️ Skipped duplicate: {job_title} - {company_name}")
        except Exception as e:
            print(f"❌ Error parsing job: {e}")

        time.sleep(1.5)

    return inserted

def crawl_all_pages(start_page: int):
    total_inserted = 0
    current_page = start_page

    while True:
        start_offset = (current_page - 1) * 10
        params = FIXED_PARAMS.copy()
        params["start"] = start_offset

        try:
            print(f"📄 Crawling page {current_page}")
            response = requests.get(BASE_URL, headers=get_headers(), params=params)

            if response.status_code == 400:
                print("🎯 Reached end of job listings (400 Bad Request)")
                break

            if response.status_code == 429:
                wait_time = random.uniform(60, 120)
                print(f"⚠️ 429 Rate limit hit. Sleeping for {wait_time:.0f} seconds...\n")
                time.sleep(wait_time)
                continue

            response.raise_for_status()

            html = response.text
            inserted = parse_jobs(html)
            total_inserted += inserted
            current_page += 1

            sleep_time = random.uniform(5, 10)
            print(f"⏳ Sleeping for {sleep_time:.2f} seconds...\n")
            time.sleep(sleep_time)

        except Exception as e:
            print(f"❌ Error on page {current_page}: {e}")
            break

    print(f"\n🎯 Total inserted into MongoDB: {total_inserted}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-page", type=int, default=1, help="Page number to start crawling from (1-based)")
    args = parser.parse_args()

    crawl_all_pages(start_page=args.start_page)
