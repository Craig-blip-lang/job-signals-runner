import os

def env(name: str) -> str:
    v = os.getenv(name, "")
    return v.strip()  # <-- removes hidden spaces/newlines

APIFY_TOKEN = env("APIFY_TOKEN")
SUPABASE_URL = env("SUPABASE_URL")
SUPABASE_SERVICE_KEY = env("SUPABASE_SERVICE_KEY")


- name: Debug - show if secrets exist (safe)
        run: |
          echo "Has APIFY_TOKEN? ${{ secrets.APIFY_TOKEN != '' }}"
          echo "Has SUPABASE_URL? ${{ secrets.SUPABASE_URL != '' }}"
          echo "Has SUPABASE_SERVICE_KEY? ${{ secrets.SUPABASE_SERVICE_KEY != '' }}"


import os
import time
import hashlib
import requests
from datetime import datetime, timezone
from dateutil.parser import isoparse

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Apify actors (from Apify API docs)
CAREER_SITE_ACTOR = "fantastic-jobs~career-site-job-listing-api"
EXPIRED_ACTOR = "fantastic-jobs~expired-jobs-api-for-career-site-job-listing-api"

# Tune these
TIME_RANGE = os.getenv("TIME_RANGE", "24h")         # "1h", "24h", "7d"
MAX_JOBS_PER_COMPANY = int(os.getenv("MAX_JOBS", "500"))
INCLUDE_AI = os.getenv("INCLUDE_AI", "false").lower() == "true"
INCLUDE_LINKEDIN = os.getenv("INCLUDE_LINKEDIN", "false").lower() == "true"

HEADERS_SUPABASE = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}

def die(msg: str):
    raise SystemExit(msg)

def ensure_env():
    missing = [k for k in ["APIFY_TOKEN", "SUPABASE_URL", "SUPABASE_SERVICE_KEY"] if not os.getenv(k)]
    if missing:
        die(f"Missing env vars: {', '.join(missing)}")

def apify_run_sync_get_items(actor: str, actor_input: dict, timeout_s: int = 180) -> list:
    """
    Calls Apify 'run-sync-get-dataset-items' endpoint and returns list of items.
    Docs show this endpoint exists for the actor. :contentReference[oaicite:3]{index=3}
    """
    url = f"https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN, "timeout": str(timeout_s)}
    r = requests.post(url, params=params, json=actor_input, timeout=timeout_s + 30)
    r.raise_for_status()
    return r.json()

def supabase_get_active_job_uids(company: str) -> set[str]:
    """
    Fetch active job_uids for a company (for NEW_JOB detection).
    Uses REST query params.
    """
    url = f"{SUPABASE_URL}/rest/v1/job_posts"
    params = {
        "select": "job_uid",
        "company": f"eq.{company}",
        "is_active": "eq.true",
        "limit": "10000",
    }
    r = requests.get(url, headers=HEADERS_SUPABASE, params=params, timeout=60)
    r.raise_for_status()
    return {row["job_uid"] for row in r.json()}

def supabase_upsert_job_posts(rows: list[dict]) -> list[dict]:
    """
    Upsert by primary key (job_uid). Returns representation (rows) back.
    """
    if not rows:
        return []

    url = f"{SUPABASE_URL}/rest/v1/job_posts"
    headers = dict(HEADERS_SUPABASE)
    headers["Prefer"] = "resolution=merge-duplicates,return=representation"
    r = requests.post(url, headers=headers, json=rows, timeout=120)
    r.raise_for_status()
    return r.json()

def supabase_insert_signals(rows: list[dict]) -> None:
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/signals"
    headers = dict(HEADERS_SUPABASE)
    # If you added unique indexes (recommended), duplicates will error.
    # We'll ignore duplicates by using 'Prefer: resolution=ignore-duplicates' if supported by your setup,
    # otherwise you can keep the signal uniqueness in SQL and accept occasional 409s.
    headers["Prefer"] = "return=minimal"
    r = requests.post(url, headers=headers, json=rows, timeout=120)
    # If duplicates occur, PostgREST may return 409; we don't want the whole run to die.
    if r.status_code in (409, 400):
        print("Signal insert warning:", r.text[:300])
        return
    r.raise_for_status()

def supabase_mark_inactive(company: str, job_uids: list[str]) -> None:
    """
    Mark given jobs inactive + update last_seen_at.
    """
    if not job_uids:
        return
    url = f"{SUPABASE_URL}/rest/v1/job_posts"
    # job_uid=in.(a,b,c)
    in_list = ",".join(job_uids)
    params = {
        "company": f"eq.{company}",
        "job_uid": f"in.({in_list})",
    }
    patch = {
        "is_active": False,
        "last_seen_at": datetime.now(timezone.utc).isoformat(),
    }
    r = requests.patch(url, headers=HEADERS_SUPABASE, params=params, json=patch, timeout=120)
    r.raise_for_status()

def safe_dt(s: str | None) -> str | None:
    if not s:
        return None
    try:
        return isoparse(s).astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def fallback_uid(company: str, job_url: str) -> str:
    """
    If the feed ever lacks 'id', create a stable id from company + url.
    """
    raw = f"{company}::{job_url}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def map_job_item_to_row(company: str, item: dict) -> dict:
    # Output fields include: id, title, organization, date_posted, date_created, url, source, source_domain, etc. :contentReference[oaicite:4]{index=4}
    job_id = item.get("id")
    job_url = item.get("url") or ""
    job_uid = str(job_id) if job_id is not None else fallback_uid(company, job_url)

    # Location: use first derived location if present
    loc = None
    countries = item.get("countries_derived") or []
    locations = item.get("locations_derived") or []
    if locations and isinstance(locations, list) and isinstance(locations[0], dict):
        city = locations[0].get("city")
        admin = locations[0].get("admin")
        country = locations[0].get("country")
        parts = [p for p in [city, admin, country] if p]
        loc = ", ".join(parts) if parts else None

    return {
        "job_uid": job_uid,
        "company": company,
        "source": "fantastic_jobs_apify",
        "source_url": job_url,
        "title": item.get("title") or "(no title)",
        "location": loc,
        "country": (countries[0] if countries else None),
        "department": None,  # optional; you can map AI taxonomies or other fields here later
        "posted_at": safe_dt(item.get("date_posted")),
        "first_seen_at": datetime.now(timezone.utc).isoformat(),
        "last_seen_at": datetime.now(timezone.utc).isoformat(),
        "is_active": True,
        "metadata": {
            "organization": item.get("organization"),
            "source": item.get("source"),
            "source_domain": item.get("source_domain"),
            "source_type": item.get("source_type"),
            "date_created": item.get("date_created"),
            "locations_derived": item.get("locations_derived"),
            "countries_derived": item.get("countries_derived"),
            "remote_derived": item.get("remote_derived"),
            "ai_taxonomies_a": item.get("ai_taxonomies_a"),
            "ai_work_arrangement": item.get("ai_work_arrangement"),
        },
    }

def build_new_job_signal(company: str, job_row: dict) -> dict:
    title = job_row["title"]
    loc = job_row.get("location")
    return {
        "account_name": company,
        "signal_type": "NEW_JOB",
        "title": f"{company} posted: {title}" + (f" ({loc})" if loc else ""),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "strength_score": 40,
        "source_url": job_row.get("source_url"),
        "metadata": {
            "job_uid": job_row["job_uid"],
            "title": title,
            "location": loc,
        },
        "job_uid": job_row["job_uid"],
    }

def build_removed_job_signal(company: str, job_uid: str) -> dict:
    return {
        "account_name": company,
        "signal_type": "JOB_REMOVED",
        "title": f"{company} job removed/expired: {job_uid}",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "strength_score": 25,
        "source_url": None,
        "metadata": {"job_uid": job_uid},
        "job_uid": job_uid,
    }

def load_companies() -> list[str]:
    with open("companies.txt", "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

def fetch_new_jobs_for_company(company: str) -> list[dict]:
    actor_input = {
        "organizationSearch": company,
        "timeRange": TIME_RANGE,
        "maximumJobs": MAX_JOBS_PER_COMPANY,
        "includeAi": INCLUDE_AI,
        "includeLinkedIn": INCLUDE_LINKEDIN,
        # You can optionally filter to ATS platforms:
        # "ats": ["workday", "greenhouse", "smartrecruiters", "lever.co"]
    }
    return apify_run_sync_get_items(CAREER_SITE_ACTOR, actor_input)

def fetch_expired_jobs_for_company(company: str) -> list[dict]:
    """
    The expired companion actor returns jobs flagged as expired.
    We'll request for the same org search. :contentReference[oaicite:5]{index=5}
    """
    actor_input = {
        "organizationSearch": company,
        "timeRange": TIME_RANGE,
        "maximumJobs": MAX_JOBS_PER_COMPANY,
    }
    return apify_run_sync_get_items(EXPIRED_ACTOR, actor_input)

def main():
    ensure_env()
    companies = load_companies()
    now = datetime.now(timezone.utc).isoformat()
    print(f"Run started: {now} | companies={len(companies)} | timeRange={TIME_RANGE}")

    total_new_signals = 0
    total_removed_signals = 0
    total_jobs_upserted = 0

    for company in companies:
        print(f"\n=== {company} ===")

        # 1) Load existing active jobs for NEW_JOB detection
        existing_active = supabase_get_active_job_uids(company)
        print(f"Existing active jobs: {len(existing_active)}")

        # 2) Fetch new/updated jobs
        items = fetch_new_jobs_for_company(company)
        print(f"Fetched items: {len(items)}")

        mapped_rows = [map_job_item_to_row(company, it) for it in items]
        # Upsert
        upserted = supabase_upsert_job_posts(mapped_rows)
        total_jobs_upserted += len(upserted)
        print(f"Upserted rows: {len(upserted)}")

        # 3) NEW_JOB signals
        new_rows = [r for r in mapped_rows if r["job_uid"] not in existing_active]
        new_signals = [build_new_job_signal(company, r) for r in new_rows]
        supabase_insert_signals(new_signals)
        total_new_signals += len(new_signals)
        print(f"NEW_JOB signals: {len(new_signals)}")

        # 4) Expired jobs + removal signals
        expired_items = fetch_expired_jobs_for_company(company)
        expired_ids = []
        for it in expired_items:
            jid = it.get("id")
            if jid is not None:
                expired_ids.append(str(jid))
        expired_ids = list(sorted(set(expired_ids)))

        if expired_ids:
            # mark inactive
            # If expired list is large, batch it
            BATCH = 200
            for i in range(0, len(expired_ids), BATCH):
                chunk = expired_ids[i:i+BATCH]
                supabase_mark_inactive(company, chunk)
                removed_signals = [build_removed_job_signal(company, uid) for uid in chunk]
                supabase_insert_signals(removed_signals)
                total_removed_signals += len(removed_signals)

            print(f"Expired jobs processed: {len(expired_ids)} (JOB_REMOVED signals created)")
        else:
            print("Expired jobs processed: 0")

        # polite pause to avoid rate limits
        time.sleep(1.2)

    print("\n=== DONE ===")
    print(f"Total jobs upserted: {total_jobs_upserted}")
    print(f"Total NEW_JOB signals: {total_new_signals}")
    print(f"Total JOB_REMOVED signals: {total_removed_signals}")

if __name__ == "__main__":
    main()
