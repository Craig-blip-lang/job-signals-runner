import os
import time
import hashlib
import uuid
import requests
from datetime import datetime, timezone
from dateutil.parser import isoparse


def env(name: str, default: str = "") -> str:
    """Read env var safely and strip whitespace/newlines."""
    return (os.getenv(name, default) or "").strip()


APIFY_TOKEN = env("APIFY_TOKEN")
SUPABASE_URL = env("SUPABASE_URL")

# ✅ Use raw Supabase service key directly (do NOT base64 decode)
SUPABASE_SERVICE_KEY = env("SUPABASE_SERVICE_KEY")

# ✅ job_posts primary key column name in Supabase
JOB_ID_COL = "id"

# Apify actors
CAREER_SITE_ACTOR = "fantastic-jobs~career-site-job-listing-api"
EXPIRED_ACTOR = "fantastic-jobs~expired-jobs-api-for-career-site-job-listing-api"

# Tune these
TIME_RANGE = env("TIME_RANGE", "24h")  # "1h", "24h", "7d"
MAX_JOBS_PER_COMPANY = int(env("MAX_JOBS", "500"))
INCLUDE_AI = env("INCLUDE_AI", "false").lower() == "true"
INCLUDE_LINKEDIN = env("INCLUDE_LINKEDIN", "false").lower() == "true"

# ✅ Turn off paid expired actor by default (you can set USE_EXPIRED_ACTOR=true later)
USE_EXPIRED_ACTOR = env("USE_EXPIRED_ACTOR", "false").lower() == "true"

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
        # Let caller decide how fatal it is (we raise)
        print("Apify call failed")
        print("Status code:", r.status_code)
        print("Actor:", actor)
        print("Request URL:", r.url)
        print("Input sent:", actor_input)
        print("Response body:", r.text[:2000])
        r.raise_for_status()

    return r.json()


def supabase_get_active_job_uids(company: str) -> set[str]:
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

    # ✅ Merge on PRIMARY KEY id (this column IS unique)
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
    """
    Supabase/PostgREST missing column error typically:
    {"message":"Could not find the 'account_name' column of 'signals' in the schema cache"}
    """
    marker = "Could not find the '"
    if marker in resp_text:
        after = resp_text.split(marker, 1)[1]
        col = after.split("'", 1)[0]
        return col.strip() or None
    return None


def _prune_rows(rows: list[dict], drop_key: str) -> list[dict]:
    if not rows:
        return rows
    return [{k: v for k, v in r.items() if k != drop_key} for r in rows]


def supabase_insert_signals(rows: list[dict]) -> None:
    """
    Inserts signals but is resilient to schema mismatches by:
    - Dropping unknown columns when PostgREST says a column doesn't exist (PGRST204).
    This lets the run finish even if signals table schema differs.
    """
    if not rows:
        return

    url = f"{SUPABASE_URL}/rest/v1/signals"
    headers = dict(HEADERS_SUPABASE)
    headers["Prefer"] = "return=minimal"

    working = rows
    for attempt in range(0, 8):  # up to 8 schema-prune retries
        r = requests.post(url, headers=headers, json=working, timeout=120)
        if r.ok:
            return

        text = r.text or ""
        # Try to auto-fix missing columns
        missing_col = _extract_missing_column_name(text)
        if missing_col:
            print(f"Signal insert: dropping missing column '{missing_col}' and retrying...")
            working = _prune_rows(working, missing_col)
            continue

        # If invalid uuid for some sent id-like field, drop obvious ones
        if "invalid input syntax for type uuid" in text:
            for candidate in ["id", "account_id", "signal_id"]:
                if any(candidate in row for row in working):
                    print(f"Signal insert: dropping '{candidate}' due to uuid error and retrying...")
                    working = _prune_rows(working, candidate)
                    break
            else:
                # nothing to drop
                print("Signal insert warning (uuid error):", text[:500])
                return
            continue

        # If still failing (400/409 etc), don't kill the whole job run
        print("Signal insert warning:", text[:800])
        return


def supabase_mark_inactive(company: str, job_ids: list[str]) -> None:
    if not job_ids:
        return
    url = f"{SUPABASE_URL}/rest/v1/job_posts"
    in_list = ",".join(job_ids)
    params = {
        "company": f"eq.{company}",
        JOB_ID_COL: f"in.({in_list})",
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

    # ✅ Stable UUID (Supabase job_posts.id is uuid)
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
        "job_uid": uid,  # ✅ required by your DB (NOT NULL)
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

    # We include a few common fields; unknown ones will be auto-dropped by supabase_insert_signals
    return {
        "account_name": company,     # may not exist (auto-dropped)
        "company": company,          # may exist in your schema
        "signal_type": "NEW_JOB",    # may not exist (auto-dropped)
        "type": "NEW_JOB",           # may exist instead of signal_type
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
        "account_name": company,      # may not exist (auto-dropped)
        "company": company,           # may exist
        "signal_type": "JOB_REMOVED", # may not exist (auto-dropped)
        "type": "JOB_REMOVED",        # may exist instead
        "title": f"{company} job removed/expired: {job_id}",
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
        "organizationSearch": [company],  # ✅ MUST be an array
        "timeRange": TIME_RANGE,
        "maximumJobs": MAX_JOBS_PER_COMPANY,
        "includeAi": INCLUDE_AI,
        "includeLinkedIn": INCLUDE_LINKEDIN,
    }
    return apify_run_sync_get_items(CAREER_SITE_ACTOR, actor_input)


def fetch_expired_jobs_for_company(company: str) -> list[dict]:
    actor_input = {
        "organizationSearch": [company],  # ✅ MUST be an array
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

        # ✅ Safety: ensure required column is always present (DB requires NOT NULL)
        for r in mapped_rows:
            if "job_uid" not in r or not r["job_uid"]:
                r["job_uid"] = r["id"]

        # Debug: prove job_uid is present
        print("Row keys check:", sorted(mapped_rows[0].keys()) if mapped_rows else [])

        upserted = supabase_upsert_job_posts(mapped_rows)
        total_jobs_upserted += len(upserted)
        print(f"Upserted rows: {len(upserted)}")

        # NEW_JOB signals: rows that weren't active before
        current_ids = {str(r[JOB_ID_COL]) for r in mapped_rows}
        new_rows = [r for r in mapped_rows if str(r[JOB_ID_COL]) not in existing_active]
        new_signals = [build_new_job_signal(company, r) for r in new_rows]
        supabase_insert_signals(new_signals)
        total_new_signals += len(new_signals)
        print(f"NEW_JOB signals: {len(new_signals)}")

        # ✅ JOB_REMOVED without paying for the expired actor:
        # Anything that *was* active but is *not* in today's fetch => mark inactive + signal
        removed_ids = sorted(existing_active - current_ids)
        if removed_ids:
            BATCH = 200
            for i in range(0, len(removed_ids), BATCH):
                chunk = removed_ids[i : i + BATCH]
                supabase_mark_inactive(company, chunk)
                removed_signals = [build_removed_job_signal(company, jid) for jid in chunk]
                supabase_insert_signals(removed_signals)
                total_removed_signals += len(removed_signals)
            print(f"Removed jobs processed (diff method): {len(removed_ids)} (JOB_REMOVED signals created)")
        else:
            print("Removed jobs processed (diff method): 0")

        # Optional: if you rent the actor later, you can turn it on via USE_EXPIRED_ACTOR=true
        if USE_EXPIRED_ACTOR:
            try:
                expired_items = fetch_expired_jobs_for_company(company)
                expired_ids = []
                for it in expired_items:
                    jid = it.get("id")
                    if jid is not None:
                        job_url = it.get("url") or ""
                        seed = f"{company}::{jid or job_url}"
                        expired_ids.append(str(uuid.uuid5(uuid.NAMESPACE_URL, seed)))
                expired_ids = sorted(set(expired_ids))

                if expired_ids:
                    BATCH = 200
                    for i in range(0, len(expired_ids), BATCH):
                        chunk = expired_ids[i : i + BATCH]
                        supabase_mark_inactive(company, chunk)
                        removed_signals = [build_removed_job_signal(company, uid) for uid in chunk]
                        supabase_insert_signals(removed_signals)
                        total_removed_signals += len(removed_signals)
                    print(f"Expired jobs processed (actor): {len(expired_ids)} (JOB_REMOVED signals created)")
                else:
                    print("Expired jobs processed (actor): 0")
            except requests.HTTPError as e:
                # Don't kill the run if actor is paid / forbidden
                print("Skipping expired actor due to error:", str(e)[:250])

        time.sleep(1.2)

    print("\n=== DONE ===")
    print(f"Total jobs upserted: {total_jobs_upserted}")
    print(f"Total NEW_JOB signals: {total_new_signals}")
    print(f"Total JOB_REMOVED signals: {total_removed_signals}")


if __name__ == "__main__":
    main()
