FROM apache/airflow:3.1.7

# install Python dependencies as root, then drop back to the airflow user
USER root

# upgrade pip tooling just to be safe
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# copy requirements and install them
COPY requirements.txt /opt/airflow/requirements.txt

# use psycopg[binary] so import psycopg works without OS packages
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    && pip install --no-cache-dir "psycopg[binary]==3.3.2"

USER airflow