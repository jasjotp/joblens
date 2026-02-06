'''
Ingest job posting data from Canada (Vancouver and Toronto) the BrightData API using LinkedIn Job Postings in Canada
'''
import logging
import requests
import json
from pathlib import Path 
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

log = logging.getLogger(__name__)

# Bright Data config 
DATASET_ID = "gd_lpfll7v5hcqtkxl6l"

def fetch_linkedin_jobs(
        api_key: str,
        cities: List[str],
        roles: List[str],
        out_dir: Path,
        time_range: str = "Past 24 hours",
        job_type: str = "Full-time",
        experience_level: str =  "Entry level",
) -> Tuple[Path, int]: 
    """
    Fetches LinkedIn roles using BrightData from the last 24 hours.
    """
    url = (
        f"https://api.brightdata.com/datasets/v3/scrape"
        f"?dataset_id={DATASET_ID}"
        f"&notify=false"
        f"&include_errors=true"
        f"&type=discover_new"
        f"&discover_by=keyword"
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # build the inputs for the API reuest (we want to focus on job postings in Vancouver and Toronto)
    inputs = []

    for city in cities:
        for role in roles: 
            inputs.append(
                {
                    "location": city,
                    "keyword": role,
                    "country": "CA",
                    "time_range": time_range,
                    "job_type": job_type,
                    "experience_level": experience_level,
                    "remote": "",
                    "company": "",
                    "location_radius": ""
                }
            )

    # after we gather all the inputs/payload to get from the API, call the API 
    payload = {"input": inputs}

    # call the BrightData API
    response = requests.post(
        url,
        headers = headers,
        json = payload
    )
    response.raise_for_status()

    # returns JSONL (one JSON Object per line) so we have to grab each line and load it into the json file 
    lines = [line for line in response.text.splitlines() if line.strip()]
    job_posting_data_24h = [json.loads(line) for line in lines] # is a list of dictionaries/JSON objects, each line represents one job posting

    out_dir.mkdir(parents = True, exist_ok = True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # save the raw JSON (immutable layer basically) with a timestamp to data/raw
    outfile = out_dir / f"linkedin_jobs_{timestamp}.json"

    with open(outfile, "w", encoding = "utf-8") as f:
        json.dump(job_posting_data_24h, f, indent = 2)

    log.info(
        "Fetched %d LinkedIn job postings (cities=%s, roles=%s). Saved to %s",
        len(job_posting_data_24h),
        cities,
        roles,
        outfile,
    )
        
    return outfile, len(job_posting_data_24h)