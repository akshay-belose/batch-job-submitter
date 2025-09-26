import os
import uuid
from google.cloud import batch_v1

def submit_batch_job(bucket: str, file_name: str):
    """
    Submits a Cloud Batch job using environment variables.
    """
    project_id = os.environ["PROJECT_ID"]
    region = os.environ["REGION"]
    batch_size = int(os.environ.get("BATCH_SIZE", "50"))
    parallelism = int(os.environ.get("PARALLELISM", "50"))
    container_image = os.environ["CONTAINER_IMAGE"]

    client = batch_v1.BatchServiceClient()
    parent = f"projects/{project_id}/locations/{region}"

    job_id = f"video-processing-{uuid.uuid4().hex[:8]}"

    # Task spec: single runnable container
    job = {
        "name": f"{parent}/jobs/{job_id}",
        "task_groups": [
            {
                "task_spec": {
                    "runnables": [
                        {
                            "container": {
                                "image_uri": container_image,
                                "commands": [
                                    "python",
                                    "/app/main.py",
                                    "--bucket", bucket,
                                    "--file", file_name,
                                    "--concurrency", str(parallelism)
                                ]
                            }
                        }
                    ]
                },
                    "task_count": 1,
                    "parallelism": parallelism
            }
        ],
        "allocation_policy": {
            "instances": [
                {"policy": {"machine_type": "e2-standard-4"}}
            ]
        },
        "logs_policy": {"destination": "CLOUD_LOGGING"}
    }

    response = client.create_job(parent=parent, job=job, job_id=job_id)
    return job_id