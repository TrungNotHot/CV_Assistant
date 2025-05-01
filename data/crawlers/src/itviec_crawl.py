import time
import re
import hashlib
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from pymongo import errors
import argparse
from mongo_config import collection

BASE_URL = "https://itviec.com/it-jobs?page={}"

def parse_posted_text(text: str) -> str:
    now = datetime.now()
    if match := re.search(r'(\d+)\s+minutes?', text):
        return (now - timedelta(minutes=int(match.group(1)))).date().isoformat()
    if match := re.search(r'(\d+)\s+hours?', text):
        return (now - timedelta(hours=int(match.group(1)))).date().isoformat()
    if match := re.search(r'(\d+)\s+days?', text):
        return (now - timedelta(days=int(match.group(1)))).date().isoformat()
    return now.date().isoformat()

def generate_hash(company, title, posted_date):
    key = f"{company}{title}{posted_date}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()

def extract_job_info(driver, url):
    try:
        driver.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "html.parser")

        job_title = soup.select_one("div.job-header-info h1").get_text(strip=True)
        company_name = soup.select_one("div.job-header-info .employer-name").get_text(strip=True)

        location_text = soup.select_one("svg use[href*='map-pin']").find_parent("div").get_text(strip=True)
        location = location_text.split(",")[-1].strip()

        posted_text = soup.find("span", string=re.compile("Posted")).get_text(strip=True)
        posted_date = parse_posted_text(posted_text)

        section = soup.select_one("section.job-content")
        blocks = section.select("div.imy-5.paragraph") if section else []
        relevant_blocks = []
        for block in blocks:
            h2 = block.select_one("h2")
            if h2 and ("job description" in h2.text.lower() or "skills" in h2.text.lower()):
                content = block.get_text(separator="\n", strip=True)
                relevant_blocks.append(f"{h2.text.strip()}:\n{content}")
        full_text = "\n\n".join(relevant_blocks)

        return {
            "job_title": job_title,
            "company_name": company_name,
            "location": location,
            "posted_date": posted_date,
            "full_text_jd": full_text
        }
    except Exception as e:
        print(f"❌ Failed to parse job detail {url}: {e}")
        return None

def get_total_pages(driver) -> int:
    driver.get(BASE_URL.format(1))
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    page_numbers = [int(a.get_text(strip=True))
                    for a in soup.select("nav.ipagination .page a")
                    if a.get_text(strip=True).isdigit()]
    max_page = max(page_numbers) if page_numbers else 1
    print(f"📊 Detected total pages: {max_page}")
    return max_page

def crawl_all_pages(start_page=1):
    print("📥 Getting total page count...")
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    driver = uc.Chrome(options=options)

    total_inserted = 0
    max_pages = get_total_pages(driver)

    for page_num in range(start_page, max_pages + 1): 
        print(f"📄 Crawling page {page_num}")
        driver.get(BASE_URL.format(page_num))
        time.sleep(3)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        job_cards = soup.select("div.ipx-4")

        for idx, job_card in enumerate(job_cards):
            try:
                h3 = job_card.select_one("h3[data-url]")
                if not h3:
                    continue
                job_url = h3["data-url"].split("?")[0]
                source_id = job_url.rstrip("/").split("-")[-1]

                job_info = extract_job_info(driver, job_url)
                if not job_info or not job_info["full_text_jd"].strip():
                    continue

                hashed = generate_hash(job_info["company_name"], job_info["job_title"], job_info["posted_date"])
                job_record = {
                    **job_info,
                    "hash": hashed,
                    "source": "itviec",
                    "source_id": source_id,
                    "url": job_url,
                }

                try:
                    collection.insert_one(job_record)
                    total_inserted += 1
                    print(f"✅ Inserted: {job_info['job_title']} - {job_info['company_name']}")
                except errors.DuplicateKeyError:
                    print(f"⚠️ Skipped duplicate: {job_info['job_title']} - {job_info['company_name']}")
            except Exception as e:
                print(f"❌ Error parsing job on page {page_num}, idx {idx}: {e}")
                continue

    driver.quit()
    print(f"\n🎯 Total inserted into MongoDB: {total_inserted}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-page", type=int, default=1, help="Page number to resume crawling from (1-based)")
    args = parser.parse_args()

    crawl_all_pages(start_page=args.start_page)

