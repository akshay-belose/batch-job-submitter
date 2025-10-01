import os
from google.cloud import storage
import pandas as pd

import requests


def trigger_airflow_dag(bucket: str, file_name: str) -> str:
    """
    Reads file from GCS, chunks it and triggers Airflow DAG
    """
    # Initialize GCS client
    storage_client = storage.Client()
    bucket_client = storage_client.bucket(bucket)
    blob = bucket_client.blob(file_name)

    # Download and read the file
    content = blob.download_as_string()
    df = pd.read_csv(content)

    # Split into chunks of 50
    chunk_size = 50
    chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

    # Trigger Airflow DAG
    airflow_endpoint = "https://your-airflow-instance/api"  #os.environ.get("AIRFLOW_API_ENDPOINT")
    dag_id = "your-dag-id"  #os.environ.get("DAG_ID")

    response = requests.post(
        f"{airflow_endpoint}/api/v1/dags/{dag_id}/dagRuns",
        json={
            "conf": {
                "data_chunks": [chunk.to_dict() for chunk in chunks]
            }
        }
    )

    if response.status_code == 200:
        return response.json()["dag_run_id"]
    else:
        raise Exception(f"Failed to trigger DAG: {response.text}")
