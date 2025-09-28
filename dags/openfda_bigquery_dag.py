from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
from calendar import monthrange
import requests
import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import get_current_context


# ====== CONFIG ======
GCP_PROJECT = "mba-cdia-enap"
BQ_DATASET = "openfda"
BQ_TABLE = "semaglutide_reactions"
GCP_CONN_ID = "google_cloud_default"
# ====================

# Teste ano

def generate_query_url(year: int, month: int) -> str:
    start_date = f"{year}{month:02d}01"
    end_day = monthrange(year, month)[1]
    end_date = f"{year}{month:02d}{end_day:02d}"
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.openfda.generic_name:%22semaglutide%22"
        f"+AND+receivedate:[{start_date}+TO+{end_date}]&limit=100"
    )

def format_fda_response(api_data):
    extracted_data = []
    for result in api_data.get("results", []):
        report_id = result.get("safetyreportid")
        receivedate = result.get("receivedate")
        country = result.get("occurcountry")
        patient = result.get("patient", {})
        age = patient.get("patientonsetage")
        sex = patient.get("patientsex")
        reactions = patient.get("reaction", [])
        for reaction in reactions:
            reaction_pt = reaction.get("reactionmeddrapt")
            extracted_data.append([report_id, country, age, sex, reaction_pt])

    df = pd.DataFrame(
        extracted_data,
        columns=[
            "safetyreportid",
            "occurcountry",
            "receivedate",        # Added here
            "patientonsetage",
            "patientsex",
            "reactionmeddrapt",
        ],
    )
    return df


@task
def fetch_openfda_data():
    ctx = get_current_context()
    year = ctx["dag_run"].conf.get("year", 2025)  # default to 2025 if not provided
    month = ctx["dag_run"].conf.get("month", 1)  # default to 01 if not provided

    # ctx = get_current_context()
    # logical_date = ctx["data_interval_start"]
    # year, month = logical_date.year, logical_date.month

    url = generate_query_url(year, month)
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"OpenFDA request failed: {e}")
        return pd.DataFrame().to_dict(orient="records")

    data = resp.json()
    df = format_fda_response(data)

    return df.to_dict(orient="records")


@task
def save_to_bigquery(records: list[dict]) -> None:
    if not records:
        print("No data to write to BigQuery for this period.")
        return

    df = pd.DataFrame(records)

    # Define table id in format project.dataset.table
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)

    # Convert DataFrame to list of dicts for insert_all method
    rows_to_insert = df.to_dict(orient="records")

    errors = bq_hook.insert_all(
        project_id=GCP_PROJECT,
        dataset_id=BQ_DATASET,
        table_id=BQ_TABLE,
        rows=rows_to_insert
    )
    if errors:
        raise RuntimeError(f"Error inserting rows into BigQuery: {errors}")
    else:
        print(f"Successfully inserted {len(rows_to_insert)} records into BigQuery table {table_id}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="fetch_openfda_data_monthly",
    description="Monthly fetch of OpenFDA semaglutide adverse events to BigQuery",
    default_args=default_args,
    schedule="@monthly",
    start_date=datetime(2023, 11, 1),
    catchup=True,
    max_active_runs=1,
    tags=["openfda", "semaglutide", "bigquery"],
)
def fetch_openfda_semaglutide_monthly_bq():
    records = fetch_openfda_data()
    save_to_bigquery(records)


dag = fetch_openfda_semaglutide_monthly_bq()
