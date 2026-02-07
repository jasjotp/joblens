'''
Loads daily JSON data from the data/raw folder into PostgreSQL. Runs a deduplication step based on job posting ID for deduplication. 
Contains deduplication by job_posting_id (and source) and are upserted into a job.job_postings table 

Deduplication is enforced at the DB level via the primary key of the job posting (source, job_posting_id).

The code is idempotent so rerunning the file by accident does not cause duplicates in the DB, as rows update deterministically.

Parsing of job posting data is defensive as it handles a file containing either a JSON or newline-delimited JSON (NDJSON) and skips malformed records but logs the reason why for follow up

Upserts data using batching with psycopg executemany for performance and fewer round trips 


'''
from __future__ import annotations 
import json 
import logging 
import os
import re 
import psycopg
from psycopg.types.json import Json
from dataclasses import dataclass 
from datetime import datetime, timezone
from pathlib import Path 
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
from dateutil.parser import isoparse 

@dataclass(frozen = True)
class Settings:
    pg_dsn: str = os.environ.get("PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")
    raw_dir: Path = Path(os.environ.get("RAW_DIR", "data/raw"))
    batch_size: int = int(os.environ.get("BATCH_SIZE", "500"))
    log_level: str = os.environ.get("LOG_LEVEL", "INFO").upper()
    source_name: str = os.environ.get("SOURCE_NAME", "linkedin")

def setup_logger(level: str) -> logging.Logger:
    logging.basicConfig(
        level = getattr(logging, level, logging.INFO),
        format = "%(asctime)s %(levelname)s %(name)s :: %(message)s"
    )

    return logging.getLogger("jobs_ingest")

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

# DB schema for job postings based on raw JSON input 
DDL = """
    CREATE SCHEMA IF NOT EXISTS jobs;

    CREATE TABLE IF NOT EXISTS jobs.job_postings (
        source          TEXT NOT NULL,
        job_posting_id  TEXT NOT NULL,
        
        url                         TEXT,
        job_title                   TEXT,
        company_name                TEXT,
        company_id                  TEXT,
        company_url                 TEXT,
        company_logo                TEXT,

        job_location                TEXT,
        job_summary                 TEXT,
        job_description_formatted   TEXT,

        job_seniority_level         TEXT,
        job_function                TEXT,
        job_employment_type         TEXT,
        job_industries              TEXT,

        job_posted_time             TEXT,
        job_posted_date             TIMESTAMPTZ,
        job_num_applicants          INTEGER,

        apply_link                   TEXT,
        is_easy_apply                BOOLEAN,
        application_availability     BOOLEAN,

        discovery_input              JSONB,
        input_payload                JSONB,

        scraped_at                  TIMESTAMPTZ,   -- from record['timestamp'] if present
        ingested_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

        raw_payload                 JSONB NOT NULL,

        PRIMARY KEY (source, job_posting_id)
    );

    -- Indexes that will speed up retrieval 
    CREATE INDEX IF NOT EXISTS ix_job_postings_company ON jobs.job_postings (company_name);
    CREATE INDEX IF NOT EXISTS ix_job_postings_posted_date ON jobs.job_postings (job_posted_date);
    CREATE INDEX IF NOT EXISTS ix_job_postings_updated_at ON jobs.job_postings (updated_at);
    CREATE INDEX IF NOT EXISTS ix_job_postings_apply_link ON jobs.job_postings (apply_link);
    CREATE INDEX IF NOT EXISTS ix_job_postings_job_title ON jobs.job_postings (job_title);
    CREATE INDEX IF NOT EXISTS ix_job_postings_job_location ON jobs.job_postings (job_location);

"""

def pg_connect(dsn: str) -> psycopg.Connection: 
    """
    Connect to Postgres using the passed in DSN
    """
    # set autocommit to False so we can do transactional batches 
    return psycopg.connect(dsn, autocommit = False)

def ensure_schema(conn: psycopg.Connection) -> None:
    """
    Create / Ensure the jobs schema and job_postings table exists 
    """
    with conn.cursor() as cur:
        cur.execute(DDL)

# ensure robust JSON reading in (allow for JSON and NDJSON dumps)
def iter_json_records(path: Path) -> Iterator[Dict[str, Any]]:
    """
    Iterates JSON and NDJSON (newline delimited JSON records), so our scaper output can be changed and is not constrained to one of JSON or NDJSON
    """
    text = path.read_text(encoding = "utf-8", errors = "replace").strip()
    if not text:
        return 

    # fastest path is if there is a JSON array file
    if text[0] == "[":
        try:
            arr = json.loads(text)
            if isinstance(arr, list):
                for item in arr: 
                    if isinstance(item, dict):
                        yield item # generator object for each posting
            return 
        except json.JSONDecodeError: 
            # fall back to NDJSON parsing below if the array is not JSON
            pass 
    
    # for NDJSON, each non empty line is a JSON object 
    for i, line in enumerate(text.splitlines(), start = 1):
        line = line.strip()
        if not line:
            continue 
        try: 
            obj = json.loads(line)
        except json.JSONDecodeError: # skip a line if there is an error parsing but log that line and is metadata
            yield {"__parse_error__": f"Invalid JSON on line {i}", "__raw_line__": line}
            continue
        if isinstance(obj, dict):
            yield obj 

def list_input_files(raw_dir: Path) -> List[Path]:
    """
    Ingest all .json files under the raw directory 
    """
    if not raw_dir.exists():
        return []
    
    files = sorted([p for p in raw_dir.rglob("*.json") if p.is_file()])
    return files 

# Normalization and Validation steps 
INT_RE = re.compile(r"^-?\d+$")

def to_int(value: Any) -> Optional[int]:
    if value is None:
        return None 
    if isinstance(value, int):
        return value 
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and INT_RE.match(value.strip()):
        return int(value.strip())
    return None

def to_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "t", "1", "yes", "y"}:
            return True
        if v in {"false", "f", "0", "no", "n"}:
            return False
    return None


def to_timestamptz(value: Any) -> Optional[datetime]:
    """
    Accept ISO8601 strings like "2026-02-04T15:54:57.763Z".
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            dt = isoparse(value.strip())
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo = timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
    return None

def validate_minimum(record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Function to validate the minimum fields to safely deduplicate nad upsert
    """
    if "__parse_error__" in record: 
        return False, record["__parse_error__"]
    
    job_posting_id = str(record.get("job_posting_id") or "").strip()
    if not job_posting_id:
        return False, "Missing job_posting_id"
    return True, None

def jsonb_or_none(value):
    '''
    Converts a value to JSONB format.
    '''
    if value is None:
        return None 
    return Json(value)

def normalize_record(record: Dict[str, Any], *, source: str) -> Dict[str, Any]:
    """
    Create a clean, typed row dictionary that matches the DB columns for normalization

    Decided to always keep raw_payload as a source of truth, and map common fields to typed columns so if the job posting JSON dumps get added more fields, these new fields will not be lost
    """
    job_posting_id = str(record.get("job_posting_id") or "").strip()

    # some records include the discovery input and input payload 
    discovery_input = record.get("discovery_input") if isinstance(record.get("discovery_input"), dict) else None
    input_payload = record.get("input") if isinstance(record.get("input"), dict) else None

    row = {
        "source": source,
        "job_posting_id": job_posting_id,

        "url": record.get("url"),
        "job_title": record.get("job_title"),
        "company_name": record.get("company_name"),
        "company_id": record.get("company_id"),
        "company_url": record.get("company_url"),
        "company_logo": record.get("company_logo"),

        "job_location": record.get("job_location"),
        "job_summary": record.get("job_summary"),
        "job_description_formatted": record.get("job_description_formatted"),

        "job_seniority_level": record.get("job_seniority_level"),
        "job_function": record.get("job_function"),
        "job_employment_type": record.get("job_employment_type"),
        "job_industries": record.get("job_industries"),

        "job_posted_time": record.get("job_posted_time"),
        "job_posted_date": to_timestamptz(record.get("job_posted_date")),
        "job_num_applicants": to_int(record.get("job_num_applicants")),

        "apply_link": record.get("apply_link"),
        "is_easy_apply": to_bool(record.get("is_easy_apply")),
        "application_availability": to_bool(record.get("application_availability")),

        "discovery_input": jsonb_or_none(discovery_input),
        "input_payload": jsonb_or_none(input_payload),

        "scraped_at": to_timestamptz(record.get("timestamp")),

        # always store the full payload/record for audit/debug purposes and future schema evolution as a source of truth
        "raw_payload": Json(record),
    }
    return row

def dedupe_in_memory(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    In-file deduplication to reduce work that DB has to do

    the DB primary key is still the main source of deduplication but this speeds things up if a lot of the daily dumps contain duplicate postings
    """
    seen: set = set()
    out: List = []

    for row in rows: 
        key = (row["source"], row["job_posting_id"])
        if key in seen:
            # if duplicates exist, just keep the latest record by scraped at time if present 
            # the DB upsert will reconcile and update the duplicates 
            continue 
        seen.add(key)
        out.append(row)
    
    return out
        
# Upsert to SQL Step 

UPSERT_SQL = """
INSERT INTO jobs.job_postings (
  source, job_posting_id,
  url, job_title, company_name, company_id, company_url, company_logo,
  job_location, job_summary, job_description_formatted,
  job_seniority_level, job_function, job_employment_type, job_industries,
  job_posted_time, job_posted_date, job_num_applicants,
  apply_link, is_easy_apply, application_availability,
  discovery_input, input_payload,
  scraped_at,
  raw_payload,
  ingested_at,
  updated_at
)
VALUES (
  %(source)s, %(job_posting_id)s,
  %(url)s, %(job_title)s, %(company_name)s, %(company_id)s, %(company_url)s, %(company_logo)s,
  %(job_location)s, %(job_summary)s, %(job_description_formatted)s,
  %(job_seniority_level)s, %(job_function)s, %(job_employment_type)s, %(job_industries)s,
  %(job_posted_time)s, %(job_posted_date)s, %(job_num_applicants)s,
  %(apply_link)s, %(is_easy_apply)s, %(application_availability)s,
  %(discovery_input)s, %(input_payload)s,
  %(scraped_at)s,
  %(raw_payload)s,
  NOW(),
  NOW()
)
ON CONFLICT (source, job_posting_id)
DO UPDATE SET
  url = EXCLUDED.url,
  job_title = EXCLUDED.job_title,
  company_name = EXCLUDED.company_name,
  company_id = EXCLUDED.company_id,
  company_url = EXCLUDED.company_url,
  company_logo = EXCLUDED.company_logo,

  job_location = EXCLUDED.job_location,
  job_summary = EXCLUDED.job_summary,
  job_description_formatted = EXCLUDED.job_description_formatted,

  job_seniority_level = EXCLUDED.job_seniority_level,
  job_function = EXCLUDED.job_function,
  job_employment_type = EXCLUDED.job_employment_type,
  job_industries = EXCLUDED.job_industries,

  job_posted_time = EXCLUDED.job_posted_time,
  job_posted_date = EXCLUDED.job_posted_date,
  job_num_applicants = EXCLUDED.job_num_applicants,

  apply_link = EXCLUDED.apply_link,
  is_easy_apply = EXCLUDED.is_easy_apply,
  application_availability = EXCLUDED.application_availability,

  discovery_input = EXCLUDED.discovery_input,
  input_payload = EXCLUDED.input_payload,

  scraped_at = COALESCE(EXCLUDED.scraped_at, jobs.job_postings.scraped_at),

  raw_payload = EXCLUDED.raw_payload,
  updated_at = NOW();
"""

def upsert_batch(conn: psycopg.Connection, batch: Sequence[Dict[str, Any]]) -> int:
    """
    Performs an atomic batch upsert
    If anything fails, the caller can rollback and continue 
    """
    if not batch:
        return 0 
    
    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, batch)
    return len(batch)

# Orchestration step 
def ingest_file(conn: psycopg.Connection, path: Path, s: Settings, log: logging.Logger) -> Dict[str, Any]:
    """
    Read the data -> validate it -> normalize -> complete in memory deduplication -> batch upsert into Postgres
    """
    parsed: List = []
    bad = 0

    for record in iter_json_records(path):
        ok, reason = validate_minimum(record)
        
        if not ok:
            bad += 1 
            log.debug("Skipping bad record in %s: %s", path.name, reason)
            continue 

        parsed.append(normalize_record(record, source = s.source_name))
    
    # if the file has no valid records, exit cleanly by returning 0s for counts 
    if not parsed:
        return {"file_records": 0, "upserted": 0, "skipped_bad": bad}
    
    # deduplicate the parsed records
    parsed = dedupe_in_memory(parsed)

    inserted = 0 
        
    # try a batch transaction for this current parsed output 
    try:
        for i in range(0, len(parsed), s.batch_size):
            batch = parsed[i : i + s.batch_size]
            inserted += upsert_batch(conn, batch)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
        
    return {"file_records": len(parsed), "upserted": inserted, "skipped_bad": bad}

def run() -> None:
    s = Settings()
    log = setup_logger(s.log_level)

    log.info("Starting ingestion. raw_dir=%s batch_size=%d source=%s", s.raw_dir, s.batch_size, s.source_name)

    files = list_input_files(s.raw_dir)
    if not files: 
        log.warning("No JSON files found under %s", s.raw_dir)
        return
    
    # connect to postgres 
    conn = pg_connect(s.pg_dsn)
    try:
        ensure_schema(conn)
        conn.commit()

        totals = {
            "files": 0,
            "upserted": 0,
            "file_records": 0,
            "skipped_bad": 0
        }

        for p in files: 
            totals["files"] += 1 
            stats = ingest_file(conn, p, s, log)
            totals["upserted"] += stats["upserted"]
            totals["file_records"] += stats["file_records"]
            totals["skipped_bad"] += stats["skipped_bad"]

            log.info(
                "Ingested %s :: file_record = %d upserted = %d skipped_bad = %d",
                p.name, stats["file_records"], stats["upserted"], stats["skipped_bad"]
            )

        log.info(
            "Done. files=%d file_records=%d upserted=%d skipped_bad=%d",
            totals["files"], totals["file_records"], totals["upserted"], totals["skipped_bad"]
        )
    
    finally:
        conn.close()

if __name__ == "__main__":
    run()