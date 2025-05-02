import argparse
import hashlib
import time
import requests
from mongo_config import collection
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

API_URL = (
    "https://api.topdev.vn/td/v2/jobs?"
    "fields[job]=id,slug,title,salary,company,extra_skills,skills_str,skills_arr,"
    "skills_ids,job_types_str,job_levels_str,job_levels_arr,job_levels_ids,"
    "addresses,status_display,detail_url,job_url,salary,published,refreshed,"
    "applied,candidate,requirements_arr,packages,benefits,content,features,"
    "is_free,is_basic,is_basic_plus,is_distinction"
    "&fields[company]=slug,tagline,addresses,skills_arr,industries_arr,"
    "industries_str,image_cover,image_galleries,benefits"
    "&locale=vi_VN&page={}"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}


def get_full_text_jd(url: str) -> str:
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    try:
        driver = uc.Chrome(options=options)
        driver.get(url)

        # Scroll to the middle of the page to trigger lazy loading
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")

        # Wait for the element having ID 'JobDescription' to be present in the DOM
        try:
            WebDriverWait(driver, 2).until(
                EC.presence_of_element_located((By.ID, "JobDescription"))
            )
        except:
            print(f"❌ Timeout: JobDescription not found in DOM via JS for: {url}")
            return ""

        driver.execute_script("""
            const elem = document.querySelector('#JobDescription');
            if (elem) {
                elem.querySelectorAll("img, svg, i, picture").forEach(e => e.remove());
            }
        """)

        # Get the text content of the element with ID 'JobDescription'
        content = driver.execute_script("""
            const elem = document.querySelector('#JobDescription');
            return elem ? elem.innerText.trim() : "";
        """)

        return content or ""
    except Exception as e:
        print(f"❌ Selenium failed for JD at {url}: {e}")
        return ""
    finally:
        try:
            driver.quit()
        except:
            pass

def crawl_jobs_from_api(page: int):
    print(f"📄 Crawling page {page}")
    response = requests.get(API_URL.format(page), headers=HEADERS)
    response.raise_for_status()
    return response.json()


def parse_jobs(api_data: dict):
    inserted = 0
    for job in api_data.get("data", []):
        job_title = job.get("title", "")
        company_name = job.get("company", {}).get("display_name", "")
        job_url = job.get("detail_url", "")
        posted_date = job.get("refreshed", {}).get("date", "")[:10]
        source_id = str(job.get("id"))

        sort_address = job.get("addresses", {}).get("sort_addresses", "")
        location = sort_address.split(",")[-1].strip() if "," in sort_address else sort_address.strip() or "Unknown"

        if not all([job_title, company_name, job_url]):
            print(f"⚠️ Missing field - title: {job_title}, company: {company_name}, url: {job_url}")
            continue

        full_text = get_full_text_jd(job_url)
        if not full_text.strip():
            print(f"⚠️ Empty JD for: {job_title} - {company_name}")
            print(f"⚠️ JD URL: {job_url}")
            continue

        hash_key = f"{company_name}{job_title}{posted_date}"
        hashed = hashlib.md5(hash_key.encode("utf-8")).hexdigest()

        job_data = {
            "job_title": job_title,
            "company_name": company_name,
            "hash": hashed,
            "location": location,
            "posted_date": posted_date,
            "source": "topdev",
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

        time.sleep(1.2)

    return inserted


def crawl_all_pages(start_page=1):
    print("📥 Getting total page count...")
    first_page = crawl_jobs_from_api(start_page)
    total_pages = first_page.get("meta", {}).get("last_page", start_page)
    print(f"📊 Total pages: {total_pages}")

    total_inserted = parse_jobs(first_page)

    for page in range(start_page + 1, total_pages + 1):
        try:
            data = crawl_jobs_from_api(page)
            total_inserted += parse_jobs(data)
        except Exception as e:
            print(f"❌ Error on page {page}: {e}")

    print(f"\n🎯 Total inserted into MongoDB: {total_inserted}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-page", type=int, default=1, help="Page number to start crawling from (1-based)")
    args = parser.parse_args()
    crawl_all_pages(start_page=args.start_page)
