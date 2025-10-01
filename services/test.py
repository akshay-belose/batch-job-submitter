from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago


# Dummy functions for tasks
def fetch_metadata(video_id, **kwargs):
    print(f"[Video {video_id}] Fetching metadata...")


def fetch_transcript(video_id, **kwargs):
    print(f"[Video {video_id}] Fetching transcript...")


def process_frames(video_id, **kwargs):
    print(f"[Video {video_id}] Processing video frames...")


def semantic_analysis(video_id, **kwargs):
    print(f"[Video {video_id}] Running semantic analysis...")


def normalization(video_id, **kwargs):
    print(f"[Video {video_id}] Running normalization...")


# Default args
default_args = {
    'owner': 'airflow',
    'retries': 1
}
# Example: 50 video IDs passed as DAG param (in real case, pass dynamically)
VIDEO_IDS = [f"video_{i}" for i in range(1, 51)]
with DAG(
        dag_id="video_processing_batch",
        default_args=default_args,
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
        params={"video_ids": VIDEO_IDS}  # Accept video ids as param
) as dag:
    for video_id in dag.params["video_ids"]:
        with TaskGroup(group_id=f"video_{video_id}_pipeline") as tg:
            # Step 1,2,3 - parallel tasks
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
            # Step 4 - waits for 1,2,3
            t4 = PythonOperator(
                task_id="semantic_analysis",
                python_callable=semantic_analysis,
                op_kwargs={"video_id": video_id},
            )
            # Step 5 - waits for 4
            t5 = PythonOperator(
                task_id="normalization",
                python_callable=normalization,
                op_kwargs={"video_id": video_id},
            )
            # Dependencies
            [t1, t2, t3] >> t4 >> t5
