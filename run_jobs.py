import os
import time
import uuid
import requests
from datetime import datetime, timezone
from dateutil.parser import isoparse


# ---- DEBUG: prove which file + commit is running ----
print("RUNNING FILE:", os.path.abspath(__file__))
print("GITHUB_SHA:", os.getenv("GITHUB_SHA", "(not set)"))


def env(name: str, default: str = "") -> str:
    """Read env var safely and strip whitespace/newlines."""
    return (os.getenv(name, default) or "").strip()


APIFY_TOKEN = env("APIFY_TOKEN")
SUPABASE_URL = env("SUPABASE_URL")
SUPABASE_SERVICE_KEY = env("SUPABASE_SERVICE_KEY")

JOB_ID_COL = "id"

# ✅ ONLY ONE Apify actor: the free one
CAREER_SITE_ACTOR = "fantastic-jobs~career-site-job-listing-api"

TIME_RANGE = env("TIME_RANGE", "24h")
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
    missing = [k for k in ["APIFY_TOKEN", "SUPABASE_URL", "SUPABASE_SERVICE_KEY"] if not env(k)]
    if missing:
        die(f"Missing env vars: {', '.join(missing)}")


def apify_run_sync_get_items(actor: str, actor_input: dict, timeout_s: int = 180) -> list:
    url = f"https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN, "timeout": str(timeout_s)}
    r = requests.post(url, params=params, json=actor_input, timeout=timeout_s + 30)

    if not r.ok:
        print("Apify call failed")
        print("Status code:", r.status_code)
        print("Actor:", actor)
        print("Request URL:", r.url)
        print("Input sent:", actor_input)
        print("Response body:", r.text[:2000])
        r.raise_for_status()

    return r.json()


def supabase_get_active_job_ids(company: str) -> set[str]:
    url = f"{SUPABASE_URL}/rest/v1/job_posts"
    params = {
        "select": JOB_ID_COL,
        "company": f"eq.{company}",
        "is_active": "eq.true",
        "limit": "10000",
    }
    r = requests.get(url, headers=HEADERS_SUPABASE, params=params, timeout=60)
    if not r.ok:
        print("Supabase GET failed")
        print("Status code:", r.status_code)
        print("Request URL:", r.url)
        print("Response body:", r.text[:1000])
        r.raise_for_status()
    return {str(row[JOB_ID_COL]) for row in r.json()}


def supabase_upsert_job_posts(rows: list[dict]) -> list[dict]:
    if not rows:
        return []

    url = f"{SUPABASE_URL}/rest/v1/job_posts"
    headers = dict(HEADERS_SUPABASE)
    headers["Prefer"] = "resolution=merge-duplicates,return=representation"

    # upsert based on PRIMARY KEY id
    params = {"on_conflict": JOB_ID_COL}

    r = requests.post(url, headers=headers, params=params, json=rows, timeout=120)
    if not r.ok:
        print("Supabase UPSERT failed")
        print("Status code:", r.status_code)
        print("Response body:", r.text[:2000])
        print("Example row keys:", sorted(list(rows[0].keys())) if rows else [])
        r.raise_for_status()
    return r.json()


def _extract_missing_column_name(resp_text: str) -> str | None:
    marker = "Could not find the '"
    if marker in resp_text:
        after = resp_text.split(marker, 1)[1]
        col = after.split("'", 1)[0]
        return col.strip() or None
    return None


def _prune_rows(rows: list[dict], drop_key: str) -> list[dict]:
    return [{k: v for k, v in r.items() if k != drop_key} for r in rows]


def supabase_insert_signals(rows: list[dict]) -> None:
    """
    Best-effort insert:
    - If your signals table is missing a column, drop it and retry.
    - Never crash the job because of signals schema mismatch.
    """
    if not rows:
        return

    url = f"{SUPABASE_URL}/rest/v1/signals"
    headers = dict(HEADERS_SUPABASE)
    headers["Prefer"] = "return=minimal"

    working = rows
    for _ in range(10):
        r = requests.post(url, headers=headers, json=working, timeout=120)
        if r.ok:
            return

        text = r.text or ""
        missing_col = _extract_missing_column_name(text)
        if missing_col:
            print(f"Signal insert: dropping missing column '{missing_col}' and retrying...")
            working = _prune_rows(working, missing_col)
            continue

        print("Signal insert warning:", text[:800])
        return


def supabase_mark_inactive(company: str, job_ids: list[str]) -> None:
    if not job_ids:
        return
    url = f"{SUPABASE_URL}/rest/v1/job_posts"
    in_list = ",".join(job_ids)
    params = {"company": f"eq.{company}", JOB_ID_COL: f"in.({in_list})"}
    patch = {"is_active": False, "last_seen_at": datetime.now(timezone.utc).isoformat()}
    r = requests.patch(url, headers=HEADERS_SUPABASE, params=params, json=patch, timeout=120)
    r.raise_for_status()


def safe_dt(s: str | None) -> str | None:
    if not s:
        return None
    try:
        return isoparse(s).astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def map_job_item_to_row(company: str, item: dict) -> dict:
    job_id = item.get("id")
    job_url = item.get("url") or ""

    # Stable UUID across runs
    seed = f"{company}::{job_id or job_url}"
    uid = str(uuid.uuid5(uuid.NAMESPACE_URL, seed))

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
        "id": uid,
        "job_uid": uid,  # NOT NULL in your DB
        "company": company,
        "title": item.get("title") or "(no title)",
        "location": loc,
        "country": (countries[0] if countries else None),
        "first_seen_at": now,
        "last_seen_at": now,
        "is_active": True,
    }


def build_new_job_signal(company: str, job_row: dict) -> dict:
    title = job_row["title"]
    loc = job_row.get("location")
    job_id = str(job_row.get("job_uid") or job_row[JOB_ID_COL])
    return {
        "account_name": company,   # might not exist; will be dropped if missing
        "company": company,
        "signal_type": "NEW_JOB",
        "type": "NEW_JOB",
        "title": f"{company} posted: {title}" + (f" ({loc})" if loc else ""),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "strength_score": 40,
        "source_url": None,
        "metadata": {"job_id": job_id, "title": title, "location": loc},
        "job_uid": job_id,
    }


def build_removed_job_signal(company: str, job_id: str) -> dict:
    job_id = str(job_id)
    return {
        "account_name": company,
        "company": company,
        "signal_type": "JOB_REMOVED",
        "type": "JOB_REMOVED",
        "title": f"{company} job removed: {job_id}",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "strength_score": 25,
        "source_url": None,
        "metadata": {"job_id": job_id},
        "job_uid": job_id,
    }


def load_companies() -> list[str]:
    with open("companies.txt", "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]


def fetch_new_jobs_for_company(company: str) -> list[dict]:
    actor_input = {
        "organizationSearch": [company],  # MUST be array
        "timeRange": TIME_RANGE,
        "maximumJobs": MAX_JOBS_PER_COMPANY,
        "includeAi": INCLUDE_AI,
        "includeLinkedIn": INCLUDE_LINKEDIN,
    }
    return apify_run_sync_get_items(CAREER_SITE_ACTOR, actor_input)


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

        existing_active = supabase_get_active_job_ids(company)
        print(f"Existing active jobs: {len(existing_active)}")

        items = fetch_new_jobs_for_company(company)
        print(f"Fetched items: {len(items)}")

        mapped_rows = [map_job_item_to_row(company, it) for it in items]
        print("Row keys check:", sorted(mapped_rows[0].keys()) if mapped_rows else [])

        upserted = supabase_upsert_job_posts(mapped_rows)
        total_jobs_upserted += len(upserted)
        print(f"Upserted rows: {len(upserted)}")

        # NEW jobs
        current_ids = {str(r[JOB_ID_COL]) for r in mapped_rows}
        new_rows = [r for r in mapped_rows if str(r[JOB_ID_COL]) not in existing_active]
        new_signals = [build_new_job_signal(company, r) for r in new_rows]
        supabase_insert_signals(new_signals)
        total_new_signals += len(new_signals)
        print(f"NEW_JOB signals: {len(new_signals)}")

        # REMOVED jobs (diff method, free)
        removed_ids = sorted(existing_active - current_ids)
        if removed_ids:
            BATCH = 200
            for i in range(0, len(removed_ids), BATCH):
                chunk = removed_ids[i : i + BATCH]
                supabase_mark_inactive(company, chunk)
                removed_signals = [build_removed_job_signal(company, jid) for jid in chunk]
                supabase_insert_signals(removed_signals)
                total_removed_signals += len(removed_signals)
            print(f"JOB_REMOVED signals: {len(removed_ids)}")
        else:
            print("JOB_REMOVED signals: 0")

        time.sleep(1.2)

    print("\n=== DONE ===")
    print(f"Total jobs upserted: {total_jobs_upserted}")
    print(f"Total NEW_JOB signals: {total_new_signals}")
    print(f"Total JOB_REMOVED signals: {total_removed_signals}")


if __name__ == "__main__":
    main()
