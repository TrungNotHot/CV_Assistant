from cpageURL import pageURL
from browser_manager import BrowserManager
import json
import os
import time
import random

base_url = 'https://www.topcv.vn/tim-viec-lam-it?type_keyword=1&page=1&sba=1'
pages = 29
output_file = "urls.json"

if not os.path.exists(output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        pass  # tạo file trống


for page_num in range(1, pages + 1):
    fetcher = pageURL(base_url, page_num)
    urls = fetcher.get_listURL()

    print(f"[✓] Page {page_num} got {len(urls)} URLs")

    with open(output_file, "a", encoding="utf-8") as f:
        for url in urls:
            f.write(json.dumps({"url": url}, ensure_ascii=False) + "\n")

    sleep_time = random.uniform(15, 40)
    fetcher.browser.close()
    print(f"[-] Sleeping {sleep_time:.1f} seconds before next page...")
    time.sleep(sleep_time)


print(f"[✓] Finished crawling {pages} pages. All URLs saved to {output_file}")
