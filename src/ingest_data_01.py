'''
Ingest job posting data from Canada (Vancouver and Toronto) the BrightData API using LinkedIn Job Postings in Canada
'''
import requests
import json
import os 
from pathlib import Path 
from datetime import datetime 
from dotenv import load_dotenv 

# load environment variables 
load_dotenv()

BRIGHTDATA_API_KEY = os.getenv("BRIGHTDATA_API_KEY")
if not BRIGHTDATA_API_KEY:
    raise EnvironmentError(
        "BRIGHTDATA_API_KEY not found. "
        "Make sure it exists in your .env file "
    )

# Bright Data config 
DATASET_ID = "gd_lpfll7v5hcqtkxl6l"

URL = (
    f"https://api.brightdata.com/datasets/v3/scrape"
    f"?dataset_id={DATASET_ID}"
    f"&notify=false"
    f"&include_errors=true"
    f"&type=discover_new"
    f"&discover_by=keyword"
)

HEADERS = {
    "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
    "Content-Type": "application/json",
}

# build the inputs for the API reuest (we want to focus on job postings in Vancouver and Toronto)
cities = ["Vancouver", "Toronto"]

roles = [
    "Machine Learning Engineer",
    "AI Engineer",
    "Data Scientist"
]

inputs = []

for city in cities:
    for role in roles: 
        inputs.append(
            {
                "location": city,
                "keyword": role,
                "country": "CA",
                "time_range": "Past 24 hours",
                "job_type": "Full-time",
                "experience_level": "Entry level",
                "remote": "",
                "company": "",
                "location_radius": ""
            }
        )

# after we gather all the inputs/payload to get from the API, call the API 
payload = {"input": inputs}

# call the BrightData API
response = requests.post(
    URL,
    headers = HEADERS,
    json = payload
)
response.raise_for_status()
print("Status:", response.status_code)
print("Content-Type:", response.headers.get("Content-Type"))

# returns JSONL (one JSON Object per line) so we have to grab each line and load it into the json file 
lines = [line for line in response.text.splitlines() if line.strip()]
job_posting_data_24h = [json.loads(line) for line in lines] # is a list of dictionaries/JSON objects, each line represents one job posting

# save the raw JSON (immutable layer basically) with a timestamp to data/raw
Path("../data/raw").mkdir(parents = True, exist_ok = True)

timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

outfile = Path(f"data/raw/linkedin_jobs_{timestamp}.json")

with open(outfile, "w", encoding = "utf-8") as f:
    json.dump(job_posting_data_24h, f, indent = 2)

print(f"\u2713 {len(job_posting_data_24h)} job listings saved to {outfile}")