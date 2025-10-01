from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import time


# Dummy functions with simulated delays
def fetch_metadata(video_id, **kwargs):
    time.sleep(1)
    print(f"[Video {video_id}] Fetching metadata...")


def fetch_transcript(video_id, **kwargs):
    time.sleep(1)
    print(f"[Video {video_id}] Fetching transcript...")


def process_frames(video_id, **kwargs):
    time.sleep(3)
    print(f"[Video {video_id}] Processing video frames...")


def semantic_analysis(video_id, **kwargs):
    time.sleep(2)
    print(f"[Video {video_id}] Running semantic analysis...")


def normalization(video_id, **kwargs):
    time.sleep(2)
    print(f"[Video {video_id}] Running normalization...")


# DAG
default_args = {"owner": "airflow", "retries": 1}

with DAG(
        dag_id="video_processing_batch",
        default_args=default_args,
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
) as dag:
    def create_pipeline(video_id: str):

        with TaskGroup(group_id=f"video_{video_id}_pipeline") as tg:
            t1 = PythonOperator(
                task_id="fetch_metadata",
                python_callable=fetch_metadata,
                op_kwargs={"video_id": video_id},
            )

            t2 = PythonOperator(
                task_id="fetch_transcript",
                python_callable=fetch_transcript,
                op_kwargs={"video_id": video_id},
            )

            t3 = PythonOperator(
                task_id="process_frames",
                python_callable=process_frames,
                op_kwargs={"video_id": video_id},
            )

            t4 = PythonOperator(
                task_id="semantic_analysis",
                python_callable=semantic_analysis,
                op_kwargs={"video_id": video_id},
            )

            t5 = PythonOperator(
                task_id="normalization",
                python_callable=normalization,
                op_kwargs={"video_id": video_id},
            )
            [t1, t2, t3] >> t4 >> t5
        return tg


    # Wrapper task: generates sub-pipelines dynamically
    def generate_tasks(**context):
        video_ids = context["dag_run"].conf.get("video_ids", [])
        if not video_ids:
            raise ValueError("No video_ids provided in DAG run config")
        for vid in video_ids:
            create_pipeline(vid)

    PythonOperator(
        task_id="generate_video_pipelines",
        python_callable=generate_tasks,
        provide_context=True,
    )
