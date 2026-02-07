FROM apache/airflow:3.1.7

# install Python dependencies as airflow user
USER airflow

# copy requirements and install them
COPY requirements.txt /opt/airflow/requirements.txt

# use psycopg[binary] so import psycopg works without OS packages
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt