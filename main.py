import os
import sys
import time
import re
import uuid
import requests
import scrapy
from io import BytesIO
from pypdf import PdfReader
from dotenv import load_dotenv

load_dotenv()

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from utils import generate, parse_llm_response
from model import connect_to_mysql, insert_job, get_all_job_links

BASE_URL = "https://haeusermann.ch"
KARRIERE_URL = "https://haeusermann.ch/de/firma/karriere/"
COMPANY_NAME = "Haeusermann + Partner"
JOB_SOURCE = "haeusermann"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}


def pdf_to_text(pdf_url):
    try:
        r = requests.get(pdf_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        reader = PdfReader(BytesIO(r.content))
        text = ""
        for page in reader.pages:
            t = page.extract_text()
            if t:
                text += t + "\n"
        return text.strip()
    except Exception as e:
        print("PDF error: " + str(e))
        return ""


def scrape_jobs():
    jobs = []
    try:
        r = requests.get(KARRIERE_URL, headers=HEADERS, timeout=15)
        s = scrapy.Selector(text=r.text)
        for cta in s.css("div.nectar-cta h5"):
            text_span = " ".join(cta.css("span.text::text").getall()).strip()
            link_text = " ".join(cta.css("a.link_text::text").getall()).strip()
            link_text = link_text.replace("\xa0", "").strip()
            href = cta.css("a.link_text::attr(href)").get()
            if not href or "pdf" not in href.lower():
                continue
            title = (text_span + " " + link_text).strip()
            pdf_url = href if href.startswith("http") else BASE_URL + href
            jobs.append({"title": title, "pdf_url": pdf_url, "job_link": pdf_url})
    except Exception as e:
        print("Scrape error: " + str(e))
    return jobs


def call_llm_with_retry(llm_input):
    for attempt in range(3):
        try:
            resp = generate(llm_input)
            return parse_llm_response(resp)
        except Exception as e:
            err = str(e)
            print("LLM error (attempt " + str(attempt+1) + "): " + err[:100])
            if "429" in err or "RESOURCE_EXHAUSTED" in err:
                match = re.search(r"retryDelay.*?(\d+)s", err)
                wait = int(match.group(1)) + 5 if match else 60
                print("Quota exceeded. Waiting " + str(wait) + "s...")
                time.sleep(wait)
            else:
                print("LLM failed. Stopping script.")
                sys.exit(1)
    print("LLM failed after 3 retries. Stopping script.")
    sys.exit(1)


def build_parsed_data(row):
    return {
        "summary": row["summary"],
        "company": {
            "name": row["company_name"],
            "industry": row["company_industry"],
            "company_type": row["company_type"],
            "company_size": row["company_size"]
        },
        "category": {
            "main_category": row["main_category"],
            "sub_category": row["sub_category"]
        },
        "location": {
            "country": row["country"],
            "state": row["state"],
            "city": row["city"],
            "postal_code": row["postal_code"]
        },
        "seniority_level": row["seniority_level"],
        "experience_min_years": row["experience_min_years"],
        "experience_max_years": row["experience_max_years"],
        "employment_type": row["employment_type"],
        "workload_min": row["workload_min"],
        "workload_max": row["workload_max"],
        "remote_type": row["remote_type"],
        "management_responsibility": row["management_responsibility"],
        "home_office_possible": row["home_office_possible"],
        "education_level": row["education_level"],
        "published_at": row["published_at"] or None,
    }


def make_row(job_id, job, parsed):
    p = parsed if parsed else {}
    c = p.get("company", {}) if p else {}
    cat = p.get("category", {}) if p else {}
    loc = p.get("location", {}) if p else {}
    return {
        "job_external_id": job_id,
        "title": job["title"],
        "job_link": job["job_link"],
        "job_source": JOB_SOURCE,
        "description": job.get("description", "")[:1000],
        "summary": p.get("summary", ""),
        "company_name": COMPANY_NAME,
        "company_industry": c.get("industry", "Rechtsberatung und Notariat"),
        "company_type": "employer",
        "company_size": "medium",
        "main_category": cat.get("main_category", "Recht"),
        "sub_category": cat.get("sub_category", "Notariat"),
        "country": "CH",
        "state": "Bern",
        "city": loc.get("city", "Bern"),
        "postal_code": loc.get("postal_code", ""),
        "seniority_level": p.get("seniority_level", "unknown"),
        "experience_min_years": p.get("experience_min_years", 0),
        "experience_max_years": p.get("experience_max_years", 0),
        "employment_type": p.get("employment_type", "unknown"),
        "workload_min": p.get("workload_min", 0),
        "workload_max": p.get("workload_max", 0),
        "remote_type": p.get("remote_type", "on-site"),
        "management_responsibility": p.get("management_responsibility", False),
        "home_office_possible": p.get("home_office_possible", False),
        "education_level": p.get("education_level", ""),
        "published_at": p.get("published_at", ""),
    }


def main():
    print("=" * 60)
    print("HAEUSERMANN SCRAPER")
    print("=" * 60)

    conn = connect_to_mysql()
    if not conn:
        print("Failed to connect to database. Exiting.")
        return

    done_job_links = get_all_job_links(conn)
    print("Already processed jobs: " + str(len(done_job_links)))

    jobs = scrape_jobs()
    print("Jobs found: " + str(len(jobs)))

    if not jobs:
        print("No jobs found.")
        conn.close()
        return

    processed = 0

    for idx, job in enumerate(jobs):
        print("\n[" + str(idx+1) + "] " + job["title"])

        if job["job_link"] in done_job_links:
            print("  Already processed - skipping")
            continue

        pdf_text = pdf_to_text(job["pdf_url"])
        job["description"] = pdf_text[:1000] if pdf_text else job["title"]

        print("  Parsing LLM...")
        parsed = call_llm_with_retry(job["description"] or job["title"])
        if not parsed:
            parsed = {}

        job_id = str(uuid.uuid5(uuid.NAMESPACE_URL, job["job_link"] + job["title"]))
        row = make_row(job_id, job, parsed)

        insert_job(conn, row["job_external_id"], row["title"], row["job_link"],
                   row["job_source"], row["description"], build_parsed_data(row))
        processed += 1
        print("  Saved!")
        time.sleep(2)

    conn.close()
    print("\n" + "=" * 60)
    print("Total processed: " + str(processed))
    print("=" * 60)


if __name__ == "__main__":
    main()
