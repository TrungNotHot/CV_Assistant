
### ✅ `TOPCV/README.md`

```markdown
# 🧠 TOPCV IT Job Crawler

This folder contains a full-featured Python crawler for collecting **IT job postings** from [TopCV.vn](https://www.topcv.vn), built with structured batch logic, retry capability, and support for MongoDB integration.

> ✅ As of now, **1,396 IT jobs** have been successfully crawled.

---

## 📁 File Structure

```
TOPCV/
├── browser_manager.py       # Browser manager using Selenium (headless-safe)
├── cpageURL.py              # Extracts paginated job listing URLs
├── cURL.py                  # Defines URLJob class for crawling job detail pages
├── crawl_URLs.py            # Gets all job URLs, handles page logic and saves to urls.json
├── crawl_Jobs.py            # Runs batch job crawling, pushes to MongoDB & JSONL
```

---

## 🚀 Features

- **Full URL pipeline**: from listing to full job detail pages
- **Built-in retry system**: detects "Just a moment..." and reruns failed URLs
- **MongoDB support**: optional push to MongoDB Atlas (or local)
- **JSONL logging**: job data saved in streaming-friendly format
- **Delay randomization**: mimics human browsing to avoid detection

---

## 🔧 Usage

### 🔹 Step 1: Crawl job URLs from TopCV listing
```bash
python crawl_URLs.py
```
- Outputs `urls.json` or `job_links.jsonl`  
- Supports multi-page crawl (e.g. IT jobs only)

---

### 🔹 Step 2: Fetch job details (with retry/resume support)
```bash
python crawl_Jobs.py
```
- Input: list of job URLs  
- Output: `topcv_jobs_details.jsonl`  
- Optional: MongoDB push (configured in `cURL.py`)

---

## 📊 Job Fields Collected

- Job title  
- Company name  
- Company scope (e.g., “5000+ employees”)  
- Company field (e.g., “IT”, “Manufacturing”)  
- Location  
- Salary  
- Experience requirement  
- Full job requirement description  
- Job post date (`date_post`)

---

## 🧠 Notes

- Targeted only for **IT-related jobs**
- `crawl_Jobs.py` supports:
  - `batch_size`, `start_batch`, `batch_limit`
  - Auto-detects "just a moment" pages and retries later
- Browser: controlled by `BrowserManager` (Selenium/Chromedriver)

---

## ☁️ MongoDB Setup (optional)

If you want to push data to MongoDB Atlas:

1. Create a user and cluster on [cloud.mongodb.com](https://cloud.mongodb.com)
2. Get the URI (e.g. `mongodb+srv://user:pass@...`)
3. Add it inside `cURL.py` (`push_to_mongo()`)

---

## 💾 Example Output

✅ 1,396 jobs saved to `topcv_jobs_details.jsonl`

Each line = 1 job, as JSON object:
```json
{
  "title": "IT Helpdesk - Hà Nội",
  "company_name": "ABC Co., Ltd",
  "company_field": "IT / Software",
  "company_scope": "1000+ nhân viên",
  "experience": "2 năm",
  "requirements": "Thành thạo phần cứng, mạng, có tư duy logic",
  "date_post": "2025-04-25",
  "url": "https://www.topcv.vn/..."
}
```



