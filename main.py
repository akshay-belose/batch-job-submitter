from fastapi import FastAPI
from pydantic import BaseModel
from services.batch_submitter import submit_batch_job
from services.airflow_trigger import trigger_airflow_dag
import logging

app = FastAPI()


class PubSubMessage(BaseModel):
    bucket: str
    name: str


@app.post("/pubsub/push")
async def pubsub_push(message: PubSubMessage):
    """
    Receives a Pub/Sub message with the file uploaded to GCS,
    and submits a Cloud Batch job for processing.
    """
    try:
        job_id = submit_batch_job(bucket=message.bucket, file_name=message.name)
        logging.info(f"Submitted Cloud Batch job: {job_id}")
        return {"status": "ok", "job_id": job_id}

    except Exception as e:
        logging.exception("Failed to submit Cloud Batch job")
        return {"status": "error", "detail": str(e)}


@app.post("/trigger-dag")
async def trigger_dag(message: PubSubMessage):
    """
    Receives a message with file details,
    processes the file and triggers an Airflow DAG.
    """
    try:
        dag_run_id = trigger_airflow_dag(bucket=message.bucket, file_name=message.name)
        logging.info(f"Triggered Airflow DAG: {dag_run_id}")
        return {"status": "ok", "dag_run_id": dag_run_id}

    except Exception as e:
        logging.exception("Failed to trigger Airflow DAG")
        return {"status": "error", "detail": str(e)}
