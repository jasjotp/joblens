from datetime import datetime
from pathlib import Path

from airflow import DAG 
from airflow.providers.standard.operators.python import PythonOperator 
from airflow.models import Variable 
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.task.trigger_rule import TriggerRule

from src.ingest_data_01 import fetch_linkedin_jobs
import pendulum
import os

def run_fetch_jobs(**context):
    '''
    calls the fetch_linkedin_jobs to fetch roles for the specified cities and roles in the last 24 hours
    '''
    # store the secret in an Airflow Variables/Connections
    api_key = Variable.get("BRIGHTDATA_API_KEY")

    dag_run = context.get("dag_run")
    config = (dag_run.config or {}) if dag_run else {} 

    # default to cities Vancouver, Toronto and Montreal so it works even without a UI configuration being sent 
    default_cities = Variable.get("JOB_CITIES", default_var = '["Vancouver","Toronto" "Montreal"]', deserialize_json = True)
    default_roles  = Variable.get("JOB_ROLES",  default_var = '["Machine Learning Engineer","AI Engineer","Data Scientist"]', deserialize_json = True)

    # cities amd roles from the UI or a manual trigger that override the default cities 
    cities = config.get("cities", default_cities)
    roles = config.get("roles", default_roles)
    
    out_dir = Path("/opt/airflow/data/raw")

    outfile, job_count = fetch_linkedin_jobs(
        api_key = api_key, 
        cities = cities,
        roles = roles,
        out_dir = out_dir,
        time_range = config.get("time_range", "Past 24 hours"),
        job_type = config.get("job_type", "Full-time"),
        experience_level = config.get("experience_level", "Entry level")
    )

    return {
        "outfile": str(outfile),
        "job_count": job_count,
        "cities": cities,
        "roles": roles
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
        to = [os.getenv("EMAIL_TO_SEND_ALERT_TO")],
        subject = "[OK] LinkedIn Jobs Fetch SUCCESS — {{ ds }}",
        html_content="""
        <h3>LinkedIn Jobs Fetch: SUCCESS</h3>

        <p><b>DAG:</b> {{ dag.dag_id }}</p>
        <p><b>Run:</b> {{ ds }} ({{ ts }})</p>

        <p><b>Job count:</b> {{ (ti.xcom_pull(task_ids='fetch_jobs') or {}).get('job_count', 'N/A') }}</p>
        <p><b>Saved file:</b> {{ (ti.xcom_pull(task_ids='fetch_jobs') or {}).get('outfile', 'N/A') }}</p>

        <hr>

        <p>
        <b>Task Logs:</b>
        <a href="{{ ti.log_url }}" target="_blank">
            View logs in Airflow
        </a>
        </p>
        """,
        conn_id = "smtp_default",
        trigger_rule = TriggerRule.ALL_SUCCESS  # only if upstream succeeded
    )

    email_failure = EmailOperator(
        task_id = "email_failure",
        to = [os.getenv("EMAIL_TO_SEND_ALERT_TO")],
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
        conn_id = "smtp_default",
        trigger_rule = TriggerRule.ONE_FAILED,  # runs if any upstream task fails
    )

    # if fetch fails or if ingest fails, email_failure runs, and if both fetch and ingest suceed; then the email is successful
    fetch_task >> ingest_task

    [fetch_task, ingest_task] >> email_success
    [fetch_task, ingest_task] >> email_failure