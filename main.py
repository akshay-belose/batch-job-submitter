from fastapi import FastAPI
from pydantic import BaseModel
from services.batch_submitter import submit_batch_job
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
 