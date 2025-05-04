import json
import time
import random
from cURL import URLJob
import os

def run_batch(
    input_file="urls.json",
    batch_size=30,
    wait_between_batch=(300, 600),
    output_detail="topcv_jobs_details.jsonl",
    start_batch=0,
    batch_limit=None
):

    if not os.path.exists(output_detail):
        with open(output_detail, "w", encoding="utf-8") as f:
            pass


    with open(input_file, "r", encoding="utf-8") as f:
        all_jobs = [json.loads(line)["url"] for line in f]

    total_jobs = len(all_jobs)
    print(f"[✓] Total jobs to process: {total_jobs}")

    current_batch = 0

    for batch_start in range(0, total_jobs, batch_size):
        if current_batch < start_batch:
            current_batch += 1
            continue  

        if batch_limit is not None and current_batch - start_batch >= batch_limit:
            print(f"[✓] Reached batch limit: {batch_limit} batches.")
            break

        batch_jobs = all_jobs[batch_start: batch_start + batch_size]
        print(f"[→] Processing batch {current_batch}: {len(batch_jobs)} jobs")

        for job_url in batch_jobs:
            try:
                job = URLJob(job_url)
                job.fetch_data()
                job.push_to_mongo()

                with open(output_detail, "a", encoding="utf-8") as f:
                    f.write(json.dumps(job.to_dict(), ensure_ascii=False) + "\n")

                print(f"[✓] Done job: {job_url}")
            except Exception as e:
                print(f"[✗] Failed job: {job_url}, error: {e}")

            time.sleep(random.uniform(1.5, 3.5))
            job.browser.close()

        print(f"[✓] Finished batch {current_batch}")

        if batch_start + batch_size < total_jobs:
            sleep_time = random.uniform(*wait_between_batch)
            print(f"[-] Sleeping {sleep_time/60:.1f} minutes before next batch...")
            time.sleep(sleep_time)

        current_batch += 1

    print("[✓] All selected batches done.")

if __name__ == "__main__":
    run_batch(
        input_file="urls.jsonl",
        batch_size=30,
        wait_between_batch=(100, 200),
        output_detail="topcv_jobs_details.jsonl",
        start_batch=31,     
        batch_limit=20     

    )
