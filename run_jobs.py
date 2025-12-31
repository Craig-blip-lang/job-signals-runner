import os
import time
import hashlib
import base64
import requests
from datetime import datetime, timezone
from dateutil.parser import isoparse


def env(name: str, default: str = "") -> str:
    """Read env var safely and strip whitespace/newlines."""
    return (os.getenv(name, default) or "").strip()


APIFY_TOKEN = env("APIFY_TOKEN")
SUPABASE_URL = env("SUPABASE_URL")

# ✅ Decode base64 Supabase service key to avoid hidden newline/header issues
SUPABASE_SERVICE_KEY_B64 = env("SUPABASE_SERVICE_KEY_B64")
try:
    SUPABASE_SERVICE_KEY = base64.b64decode(SUPABASE_SERVICE_KEY_B64).decode("utf-8").strip()
except Exception as e:
    raise SystemExit(f"Failed to decode SUPABASE_SERVICE_KEY_B64 (is it valid base64?): {e}")

# Apify actors
CAREER_SITE_ACTOR = "fantastic-jobs~career-site-job-listing-api"
EXPIRED_ACTOR = "fantastic-jobs~expired-jobs-api-for-career-site-job-listing-api"

# Tune these
TIME_RANGE = env("TIME_RANGE", "24h")  # "1h", "24h", "7d"
MAX_JOBS_PER_COMPANY = int(env("MAX_JOBS", "500"))
INCLUDE_AI = env("INCLUDE_AI", "false").lower() == "true"
INCLUDE_LINKEDIN = env("INCLUDE_LINKEDIN", "false").lower() == "true"

HEADERS_SUPABASE = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}


def die(msg: str):
    raise SystemExit(msg)


def ensure_env():
    missing = [k for k in ["APIFY_TOKEN", "SUPABASE_URL", "SUPABASE_SERVICE_KEY_B64"] if not env(k)]
    if missing:
        die(f"Missing env vars: {', '.join(missing)}")


def apify_run_sync_get_items(actor: str, actor_input: dict, timeout_s: int = 180) -> list:
    url = f"https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN, "timeout": str(timeout_s)}
    r = requests.post(url, params=params, json=actor_input, timeout=timeout_s + 30)
    r.raise_for_status()
    return r.json()


def supabase_get_active_job_uids(company: str) -> set[str]:
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
    headers["Prefer"] = "return=minimal"
    r = requests.post(url, headers=headers, json=rows, timeout=120)
    if r.status_code in (409, 400):
        print("Signal insert warning:", r.text[:300])
        return
    r.raise_for_status()


def supabase_mark_inactive(company: str, job_uids: list[str]) -> None:
    if not job_uids:
        return
    url = f"{SUPABASE_URL}/rest/v1/job_posts"
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
    raw = f"{company}::{job_url}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def map_job_item_to_row(company: str, item: dict) -> dict:
    job_id = item.get("id")
    job_url = item.get("url") or ""
    job_uid = str(job_id) if job_id is not None else fallback_uid(company, job_url)

    loc = None
    countries = item.get("countries_derived") or []
    locations = item.get("locations_derived") or []
    if locations and isinstance(locations, list) and isinstance(locations[0], dict):
        city = locations[0].get("city")
        admin = locations[0].get("admin")
        country = locations[0].get("country")
        parts = [p for p in [city, admin, country] if p]
        loc = ", ".join(parts) if parts else None

    now = datetime.now(timezone.utc).isoformat()
    return {
        "job_uid": job_uid,
        "company": company,
        "source": "fantastic_jobs_apify",
        "source_url": job_url,
        "title": item.get("title") or "(no title)",
        "location": loc,
        "country": (countries[0] if countries else None),
        "department": None,
        "posted_at": safe_dt(item.get("date_posted")),
        "first_seen_at": now,
        "last_seen_at": now,
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
        "metadata": {"job_uid": job_row["job_uid"], "title": title, "location": loc},
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
    }
    return apify_run_sync_get_items(CAREER_SITE_ACTOR, actor_input)


def fetch_expired_jobs_for_company(company: str) -> list[dict]:
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

        existing_active = supabase_get_active_job_uids(company)
        print(f"Existing active jobs: {len(existing_active)}")

        items = fetch_new_jobs_for_company(company)
        print(f"Fetched items: {len(items)}")

        mapped_rows = [map_job_item_to_row(company, it) for it in items]
        upserted = supabase_upsert_job_posts(mapped_rows)
        total_jobs_upserted += len(upserted)
        print(f"Upserted rows: {len(upserted)}")

        new_rows = [r for r in mapped_rows if r["job_uid"] not in existing_active]
        new_signals = [build_new_job_signal(company, r) for r in new_rows]
        supabase_insert_signals(new_signals)
        total_new_signals += len(new_signals)
        print(f"NEW_JOB signals: {len(new_signals)}")

        expired_items = fetch_expired_jobs_for_company(company)
        expired_ids = []
        for it in expired_items:
            jid = it.get("id")
            if jid is not None:
                expired_ids.append(str(jid))
        expired_ids = sorted(set(expired_ids))

        if expired_ids:
            BATCH = 200
            for i in range(0, len(expired_ids), BATCH):
                chunk = expired_ids[i : i + BATCH]
                supabase_mark_inactive(company, chunk)
                removed_signals = [build_removed_job_signal(company, uid) for uid in chunk]
                supabase_insert_signals(removed_signals)
                total_removed_signals += len(removed_signals)
            print(f"Expired jobs processed: {len(expired_ids)} (JOB_REMOVED signals created)")
        else:
            print("Expired jobs processed: 0")

        time.sleep(1.2)

    print("\n=== DONE ===")
    print(f"Total jobs upserted: {total_jobs_upserted}")
    print(f"Total NEW_JOB signals: {total_new_signals}")
    print(f"Total JOB_REMOVED signals: {total_removed_signals}")


if __name__ == "__main__":
    main()
