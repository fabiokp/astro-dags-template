from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
from calendar import monthrange
import requests
import pandas as pd
import pendulum
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# ====== CONFIG ======
GCP_PROJECT = "mba-cdia-enap"
BQ_DATASET = "openfda"
BQ_TABLE = "semaglutide_reactions"
GCP_CONN_ID = "google_cloud_default"
# ====================
#testrunnn

def generate_query_url(year: int, month: int) -> str:
    start_date = f"{year}{month:02d}01"
    end_day = monthrange(year, month)[1]
    end_date = f"{year}{month:02d}{end_day:02d}"
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.openfda.generic_name:%22semaglutide%22"
        f"+AND+receivedate:[{start_date}+TO+{end_date}]"
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
            extracted_data.append(
                [report_id, country, receivedate, age, sex, reaction_pt]
            )

    df = pd.DataFrame(
        extracted_data,
        columns=[
            "safetyreportid",
            "occurcountry",
            "receivedate",
            "patientonsetage",
            "patientsex",
            "reactionmeddrapt",
        ],
    )
    return df


@task
def fetch_openfda_data():
    ctx = get_current_context()
    year = ctx["dag_run"].conf.get("year", 2025)
    month = ctx["dag_run"].conf.get("month", 1)

    url = generate_query_url(year, month)
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"OpenFDA request failed: {e}")
        return pd.DataFrame()

    data = resp.json()
    df = format_fda_response(data)

    return df


@task
def save_to_bigquery(df: pd.DataFrame):
    if df.empty:
        print("No data to write to BigQuery for this period.")
        return

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()
    destination_table = f"{BQ_DATASET}.{BQ_TABLE}"

    table_schema = [
        {"name": "safetyreportid", "type": "STRING"},
        {"name": "occurcountry", "type": "STRING"},
        {"name": "receivedate", "type": "STRING"},
        {"name": "patientonsetage", "type": "INTEGER"},
        {"name": "patientsex", "type": "STRING"},
        {"name": "reactionmeddrapt", "type": "STRING"},
    ]

    # Append data to BigQuery table, creating if necessary
    df.to_gbq(
        destination_table=destination_table,
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=credentials,
        table_schema=table_schema,
        progress_bar=False,
    )

    print(f"Loaded {len(df)} records to {GCP_PROJECT}.{destination_table}")


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="fetch_openfda_data_monthly",
    description="Monthly fetch of OpenFDA semaglutide adverse events to BigQuery",
    default_args=DEFAULT_ARGS,
    schedule="@monthly",
    start_date=pendulum.datetime(2023, 11, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["openfda", "semaglutide", "bigquery"],
)

def fetch_openfda_semaglutide_monthly():
    df = fetch_openfda_data()
    save_to_bigquery(df)

dag = fetch_openfda_semaglutide_monthly()
