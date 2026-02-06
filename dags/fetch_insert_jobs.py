from datetime import datetime
from pathlib import Path

from airflow import DAG 
from airflow.operators.python import PythonOperator 
from airflow.models import Variable 
from airflow.operators.email import EmailOperator 
from airflow.utils.trigger_rule import TriggerRule

from src.ingest_data_01 import fetch_linkedin_jobs
import pendulum

def run_fetch_jobs(**context):
    '''
    calls the fetch_linkedin_jobs to fetch roles for the specified cities and roles in the last 24 hours
    '''
    # store the secret in an Airflow Variables/Connections
    api_key = Variable.get("BRIGHTDATA_API_KEY")

    cities = ["Vancouver", "Toronto"]
    roles = ["Machine Learning Engineer", "AI Engineer", "Data Scientist"]

    out_dir = Path("/opt/airflow/data/raw")

    outfile, job_count = fetch_linkedin_jobs(
        api_key = api_key, 
        cities = cities,
        roles = roles,
        out_dir = out_dir,
        time_range = "Past 24 hours",
        job_type = "Full-time",
        experience_level = "Entry level"
    )

    return {
        "outfile": str(outfile),
        "job_count": job_count
    }

def run_ingest_postgres(**context):
    """
    Ingests the single JSON file produced by into Postgres (idempotently so the state does not change if it is ran by accident multiple times)
    """
    ti = context["ti"]
    x = ti.xcom_pull(task_ids = "fetch_jobs") or {}
    outfile = x.get("outfile") # JSON file we just fetched
    if not outfile:
        raise ValueError("fetch_jobs did not return 'outfile' in XCom")
    
    pg_dsn = Variable.get("PG_DSN")

    from src.load_data_to_db_02 import Settings, setup_logger, pg_connect, ensure_schema, ingest_file

    # build settings with our latest DSN 
    s = Settings(pg_dsn = pg_dsn)

    log = setup_logger(s.log_level)

    conn = pg_connect(s.pg_dsn)
    try:
        ensure_schema(conn)
        conn.commit()

        stats = ingest_file(conn, Path(outfile), s, log)
        conn.commit()

        return stats
    finally:
        conn.close()

# am in Pacific Time and typically am a night own, so will prefer that the DAG runs at 12AM daily (laptop is typically on then)
with DAG(
    dag_id = "linkedin_jobs_fetcher",
    start_date = pendulum.datetime(2026, 2, 6, tz = "America/Los_Angeles"),
    schedule = "0 0 * * *",  # every day at 12AM
    catchup = False,
    tags = ["linkedin", "jobs", "job postings"],
) as dag:

    fetch_task = PythonOperator(
        task_id = "fetch_jobs",
        python_callable = run_fetch_jobs,
    )

    ingest_task = PythonOperator(
        task_id = "ingest_postgres",
        python_callable = run_ingest_postgres,
    )

    # Email if task was successful or unsuccessful
    email_success = EmailOperator(
        task_id = "email_success",
        to = [Variable.get("ALERT_EMAIL_TO")],
        subject = "[OK] LinkedIn Jobs Fetch SUCCESS — {{ ds }}",
        html_content="""
        <h3>LinkedIn Jobs Fetch: SUCCESS</h3>

        <p><b>DAG:</b> {{ dag.dag_id }}</p>
        <p><b>Run:</b> {{ ds }} ({{ ts }})</p>

        <p><b>Job count:</b> {{ ti.xcom_pull(task_ids='fetch_jobs')['job_count'] }}</p>
        <p><b>Saved file:</b> {{ ti.xcom_pull(task_ids='fetch_jobs')['outfile'] }}</p>

        <hr>

        <p>
        <b>Task Logs:</b>
        <a href="{{ ti.log_url }}" target="_blank">
            View logs in Airflow
        </a>
        </p>
        """,
        trigger_rule = TriggerRule.ALL_SUCCESS,  # only if upstream succeeded
    )

    email_failure = EmailOperator(
        task_id = "email_failure",
        to = [Variable.get("ALERT_EMAIL_TO")],
        subject="[FAILED] LinkedIn Jobs Fetch FAILED — {{ ds }}",
        html_content="""
        <h3>LinkedIn Jobs Fetch: FAILED</h3>

        <p><b>DAG:</b> {{ dag.dag_id }}</p>
        <p><b>Run:</b> {{ ds }} ({{ ts }})</p>

        <p><b>Job count:</b> {{ (ti.xcom_pull(task_ids='fetch_jobs') or {}).get('job_count', 'N/A') }}</p>
        <p><b>Saved file:</b> {{ (ti.xcom_pull(task_ids='fetch_jobs') or {}).get('outfile', 'N/A') }}</p>

        <hr>

        <p><b>Task Logs:</b> <a href="{{ ti.log_url }}" target="_blank">View logs in Airflow</a></p>
        """,
        trigger_rule = TriggerRule.ONE_FAILED,  # runs if any upstream task fails
    )

    fetch_task >> ingest_task >> [email_success, email_failure]