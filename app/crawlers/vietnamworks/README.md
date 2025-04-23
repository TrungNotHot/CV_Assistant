# VietnamWorks Job Crawler

This module extracts full job descriptions from VietnamWorks by combining:

1. **VietnamWorks Search API**: to get job listing metadata.
2. **HTML parsing**: to fetch and extract full job descriptions from job detail pages.

---

## 📁 Project Structure

```
app/
  crawlers/
    vietnamworks/
      __init__.py           # Marks this as a Python module
      crawl.py              # Main crawling script
      config.py             # Config constants like headers, API URL, etc.
  data/
    vietnamworks_all_jobs.json   # Output file after crawling
```

---

## 🧪 Output

```json
{
    "job_title": "Business Analyst (Senior)",
    "company": "FPT Telecom",
    "location": "Hà Nội",
    "full_text_jd": "- Analyze and clarify business requirements..."
}
```
