"""
collector.py
============
Collects job postings from USAJobs and Jooble APIs,
publishes each record to a Pub/Sub topic, and writes
a raw JSON snapshot to GCS.

Designed to run as a Cloud Run Job triggered by Cloud Scheduler
(weekly) or manually via `gcloud run jobs execute`.

Environment variables (set via Secret Manager + Cloud Run):
  USAJOBS_EMAIL     - registered email for USAJobs API
  USAJOBS_KEY       - USAJobs API key
  JOOBLE_KEY        - Jooble API key
  GCP_PROJECT_ID    - GCP project ID
  PUBSUB_TOPIC      - full topic path: projects/{proj}/topics/{topic}
  GCS_BUCKET        - GCS bucket name (without gs://)
  GCS_RAW_PREFIX    - prefix for raw JSON (default: raw)
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Config from env ───────────────────────────────────────────────────────────
USAJOBS_EMAIL    = os.environ.get("USAJOBS_EMAIL", "")
USAJOBS_KEY      = os.environ.get("USAJOBS_KEY", "")
JOOBLE_KEY       = os.environ.get("JOOBLE_KEY", "")
GCP_PROJECT_ID   = os.environ.get("GCP_PROJECT_ID", "")
PUBSUB_TOPIC     = os.environ.get("PUBSUB_TOPIC", f"projects/{GCP_PROJECT_ID}/topics/job-postings-raw")
GCS_BUCKET       = os.environ.get("GCS_BUCKET", f"{GCP_PROJECT_ID}-job-pipeline")
GCS_RAW_PREFIX   = os.environ.get("GCS_RAW_PREFIX", "raw")

CURRENT_EPOCH    = datetime(2026, 1, 1)
RUN_TS           = datetime.now().strftime("%Y%m%d_%H%M")
USAJOBS_PAGE_SIZE = 25
USAJOBS_MAX_PAGES = 10
JOOBLE_MAX_PAGES  = 5
USAJOBS_DELAY     = 1.0
JOOBLE_DELAY      = 0.8
MIN_SCORE         = 2

USAJOBS_QUERIES = {
    "CS": [
        "software engineer","computer scientist","information technology specialist",
        "cybersecurity analyst","cloud engineer","devops engineer","systems administrator",
        "network engineer","platform engineer","site reliability engineer",
        "information security officer","systems engineer",
    ],
    "MBA": [
        "management analyst","program analyst","operations research analyst",
        "financial analyst","budget analyst","management and program analyst",
        "administrative officer","program manager","strategy analyst","policy analyst",
    ],
    "DATA_ANALYTICS": [
        "data analyst","statistician","data scientist","business intelligence analyst",
        "quantitative analyst","data engineer","reporting analyst","analytics engineer",
        "senior data analyst","intelligence analyst",
    ],
    "AI": [
        "artificial intelligence","machine learning engineer","natural language processing",
        "computer vision engineer","applied scientist","MLOps engineer",
        "deep learning engineer","AI researcher","research scientist",
    ],
}

JOOBLE_QUERIES = {
    "CS": [
        "software engineer","DevOps engineer","cloud engineer","backend developer",
        "full stack developer","cybersecurity analyst","systems architect",
        "platform engineer","solutions architect","SRE engineer",
    ],
    "MBA": [
        "strategy consultant","product manager","business analyst","operations manager",
        "management consultant","financial analyst MBA","corporate strategy","program manager",
    ],
    "DATA_ANALYTICS": [
        "data analyst","business intelligence analyst","analytics engineer","data engineer",
        "BI developer","quantitative analyst","data visualization analyst","senior data analyst",
    ],
    "AI": [
        "machine learning engineer","AI researcher","NLP engineer","MLOps engineer",
        "generative AI engineer","applied scientist","deep learning engineer",
        "LLM engineer","AI product manager",
    ],
}

# ── GCP clients ───────────────────────────────────────────────────────────────
publisher      = pubsub_v1.PublisherClient()
storage_client = storage.Client(project=GCP_PROJECT_ID)


# ── Utility functions ─────────────────────────────────────────────────────────
def clean_html(text):
    if not text or not isinstance(text, str):
        return ""
    return BeautifulSoup(text, "html.parser").get_text(separator=" ", strip=True)


def make_hash(title, company, location):
    key = f"{str(title).lower().strip()}|{str(company).lower().strip()}|{str(location).lower().strip()}"
    return hashlib.sha1(key.encode()).hexdigest()[:12]


def classify_era(date_str):
    if not date_str:
        return "unknown"
    try:
        dt = datetime.fromisoformat(str(date_str)[:10])
        return "current" if dt >= CURRENT_EPOCH else "historical"
    except Exception:
        return "unknown"


def normalize_location(raw):
    if not raw:
        return "Unknown, US"
    raw = str(raw).strip()
    if ", US" in raw:
        return raw
    STATE_MAP = {
        "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA",
        "Colorado":"CO","Connecticut":"CT","Delaware":"DE","Florida":"FL","Georgia":"GA",
        "Hawaii":"HI","Idaho":"ID","Illinois":"IL","Indiana":"IN","Iowa":"IA",
        "Kansas":"KS","Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD",
        "Massachusetts":"MA","Michigan":"MI","Minnesota":"MN","Mississippi":"MS",
        "Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV",
        "New Hampshire":"NH","New Jersey":"NJ","New Mexico":"NM","New York":"NY",
        "North Carolina":"NC","North Dakota":"ND","Ohio":"OH","Oklahoma":"OK",
        "Oregon":"OR","Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC",
        "South Dakota":"SD","Tennessee":"TN","Texas":"TX","Utah":"UT",
        "Vermont":"VT","Virginia":"VA","Washington":"WA","West Virginia":"WV",
        "Wisconsin":"WI","Wyoming":"WY","District of Columbia":"DC",
    }
    for full, abbr in STATE_MAP.items():
        if full in raw:
            parts = raw.split(",")
            city = parts[0].strip() if len(parts) > 1 else ""
            return f"{city}, {abbr}, US" if city else f"{abbr}, US"
    if "remote" in raw.lower():
        return "Remote, US"
    return f"{raw}, US"


def publish_record(record):
    """Publish a single enriched record to Pub/Sub."""
    data = json.dumps(record, ensure_ascii=False).encode("utf-8")
    future = publisher.publish(PUBSUB_TOPIC, data=data)
    return future.result()


def upload_to_gcs(records, source_name):
    """Upload raw JSON list to GCS."""
    blob_name = f"{GCS_RAW_PREFIX}/{source_name}/{source_name}_raw_{RUN_TS}.json"
    bucket    = storage_client.bucket(GCS_BUCKET)
    blob      = bucket.blob(blob_name)
    blob.upload_from_string(
        json.dumps(records, indent=2, ensure_ascii=False),
        content_type="application/json",
    )
    logger.info(f"Uploaded {len(records)} records → gs://{GCS_BUCKET}/{blob_name}")
    return f"gs://{GCS_BUCKET}/{blob_name}"


# ── USAJobs collector ─────────────────────────────────────────────────────────
USAJOBS_ENDPOINT = "https://data.usajobs.gov/api/search"


def usajobs_get_page(keyword, page=1):
    if not (USAJOBS_KEY and USAJOBS_EMAIL):
        return []
    headers = {
        "Host": "data.usajobs.gov",
        "User-Agent": USAJOBS_EMAIL,
        "Authorization-Key": USAJOBS_KEY,
    }
    params = {
        "Keyword": keyword,
        "ResultsPerPage": USAJOBS_PAGE_SIZE,
        "Page": page,
        "CountrySubDivisionCode": "US",
    }
    for attempt in range(3):
        try:
            r = requests.get(USAJOBS_ENDPOINT, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            return r.json().get("SearchResult", {}).get("SearchResultItems", [])
        except requests.exceptions.HTTPError:
            if r.status_code == 429:
                wait = (2 ** attempt) * 10
                logger.warning(f"USAJobs rate limited — waiting {wait}s")
                time.sleep(wait)
            else:
                logger.error(f"USAJobs HTTP {r.status_code} — {keyword} p{page}")
                return []
        except Exception as e:
            logger.error(f"USAJobs error: {e}")
            time.sleep(5)
    return []


def parse_usajobs_item(item, keyword, domain):
    mv       = item.get("MatchedObjectDescriptor", {})
    title    = mv.get("PositionTitle", "")
    org      = mv.get("OrganizationName", "Federal Agency")
    locs     = mv.get("PositionLocation", [])
    loc_raw  = locs[0].get("LocationName", "") if locs else ""
    qual     = mv.get("QualificationSummary", "")
    ua       = mv.get("UserArea", {}).get("Details", {})
    duties   = ua.get("MajorDuties", [])
    desc     = clean_html(qual + " " + (" ".join(duties) if isinstance(duties, list) else str(duties)))
    pub_date = mv.get("PublicationStartDate", "")[:10]
    close_dt = mv.get("ApplicationCloseDate", "")[:10]
    url      = mv.get("PositionURI", "")
    rem      = mv.get("PositionRemuneration", [])
    salary   = ""
    if rem:
        lo = rem[0].get("MinimumRange", "")
        hi = rem[0].get("MaximumRange", "")
        iv = rem[0].get("RateIntervalCode", "")
        salary = f"${lo}-${hi} {iv}".strip()
    sched    = mv.get("PositionSchedule", [])
    job_type = sched[0].get("Name", "") if sched else ""
    grades   = mv.get("JobGrade", [])
    grade    = ", ".join(g.get("Code", "") for g in grades)
    return {
        "hash_id":          make_hash(title, org, loc_raw),
        "job_title":        title,
        "company":          org,
        "location":         normalize_location(loc_raw),
        "domain":           domain,
        "era":              classify_era(pub_date),
        "publication_date": pub_date,
        "application_close":close_dt,
        "salary":           salary,
        "job_type":         job_type,
        "grade":            grade,
        "job_url":          url,
        "description":      desc[:600],
        "source":           "USAJobs",
        "search_keyword":   keyword,
        "run_timestamp":    RUN_TS,
    }


def collect_usajobs():
    all_records, seen = [], set()
    for domain, keywords in USAJOBS_QUERIES.items():
        for keyword in keywords:
            logger.info(f"USAJobs [{domain}] — {keyword}")
            for page in range(1, USAJOBS_MAX_PAGES + 1):
                items = usajobs_get_page(keyword, page)
                if not items:
                    break
                for item in items:
                    mv    = item.get("MatchedObjectDescriptor", {})
                    title = mv.get("PositionTitle", "")
                    org   = mv.get("OrganizationName", "")
                    locs  = mv.get("PositionLocation", [])
                    loc   = locs[0].get("LocationName", "") if locs else ""
                    hid   = make_hash(title, org, loc)
                    if hid in seen:
                        continue
                    seen.add(hid)
                    rec = parse_usajobs_item(item, keyword, domain)
                    all_records.append(rec)
                    publish_record(rec)
                time.sleep(USAJOBS_DELAY)
    gcs_uri = upload_to_gcs(all_records, "usajobs")
    logger.info(f"USAJobs: {len(all_records)} records collected. GCS: {gcs_uri}")
    return all_records


# ── Jooble collector ──────────────────────────────────────────────────────────
def jooble_get_page(keyword, page=1, location="United States"):
    if not JOOBLE_KEY:
        return []
    url     = f"https://jooble.org/api/{JOOBLE_KEY}"
    payload = {"keywords": keyword, "location": location, "page": page}
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=30)
            r.raise_for_status()
            return r.json().get("jobs", [])
        except requests.exceptions.HTTPError:
            logger.error(f"Jooble HTTP {r.status_code} — {keyword} p{page}")
            return []
        except Exception as e:
            logger.error(f"Jooble error: {e}")
            time.sleep(5)
    return []


def parse_jooble_job(job, keyword, domain):
    title    = job.get("title", "")
    company  = job.get("company", "Unknown Company")
    loc_raw  = job.get("location", "")
    snippet  = clean_html(job.get("snippet", "") or job.get("description", ""))
    pub_date = str(job.get("updated", ""))[:10]
    return {
        "hash_id":          make_hash(title, company, loc_raw),
        "job_title":        title,
        "company":          company,
        "location":         normalize_location(loc_raw),
        "domain":           domain,
        "era":              classify_era(pub_date),
        "publication_date": pub_date,
        "application_close":"",
        "salary":           str(job.get("salary", "")),
        "job_type":         str(job.get("type", "")),
        "grade":            "",
        "job_url":          str(job.get("link", "")),
        "description":      snippet[:600],
        "source":           "Jooble",
        "search_keyword":   keyword,
        "run_timestamp":    RUN_TS,
    }


def collect_jooble():
    all_records, seen_fp = [], set()
    for domain, keywords in JOOBLE_QUERIES.items():
        for keyword in keywords:
            logger.info(f"Jooble [{domain}] — {keyword}")
            for page in range(1, JOOBLE_MAX_PAGES + 1):
                jobs = jooble_get_page(keyword, page=page)
                if not jobs:
                    break
                for job in jobs:
                    fp = f"{job.get('title','').lower()}|{job.get('company','').lower()}|{job.get('location','').lower()}"
                    if fp in seen_fp:
                        continue
                    seen_fp.add(fp)
                    rec = parse_jooble_job(job, keyword, domain)
                    all_records.append(rec)
                    publish_record(rec)
                time.sleep(JOOBLE_DELAY)
    gcs_uri = upload_to_gcs(all_records, "jooble")
    logger.info(f"Jooble: {len(all_records)} records collected. GCS: {gcs_uri}")
    return all_records


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    logger.info(f"=== Job Collector started — run {RUN_TS} ===")
    logger.info(f"Pub/Sub topic : {PUBSUB_TOPIC}")
    logger.info(f"GCS bucket    : {GCS_BUCKET}")

    usa_records    = collect_usajobs()
    jooble_records = collect_jooble()
    total          = len(usa_records) + len(jooble_records)

    logger.info(f"=== Collection complete ===")
    logger.info(f"  USAJobs : {len(usa_records):,}")
    logger.info(f"  Jooble  : {len(jooble_records):,}")
    logger.info(f"  Total   : {total:,} records published to Pub/Sub")

    # Write combined summary to GCS
    summary = {
        "run_timestamp": RUN_TS,
        "usajobs_count": len(usa_records),
        "jooble_count":  len(jooble_records),
        "total":         total,
        "pubsub_topic":  PUBSUB_TOPIC,
    }
    bucket = storage_client.bucket(GCS_BUCKET)
    bucket.blob(f"metadata/run_summary_{RUN_TS}.json").upload_from_string(
        json.dumps(summary, indent=2), content_type="application/json"
    )
    logger.info(f"Run summary saved to GCS: metadata/run_summary_{RUN_TS}.json")


if __name__ == "__main__":
    main()
