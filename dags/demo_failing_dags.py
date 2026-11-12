"""Demo DAGs for testing the Neops failure listener. Trigger them manually from the UI.

  demo_failing_python  ValueError in `transform`; `load` ends upstream_failed (expect ONE event)
  demo_failing_bash    exit 3; the useful text ("disk full") is only in the task log (tests log enrichment)
  demo_failing_retry   fails on every try with 2 retries (shows how many events a retried task sends)
  demo_failing_mapped  3 mapped task instances, 2 fail (expect one event each)
  demo_success         succeeds (expect no event)

Copy to the Airflow dags folder (on Astro: the project's dags/ folder).
"""
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import DAG, task
except ImportError:  # Airflow 2.x
    from airflow import DAG
    from airflow.decorators import task

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator

COMMON = dict(start_date=datetime(2026, 1, 1), schedule=None, catchup=False, tags=["neops-demo"])

with DAG(dag_id="demo_failing_python", **COMMON):

    @task
    def extract():
        return "rows"

    @task
    def transform(data):
        raise ValueError("Demo failure: column 'order_id' contains nulls")

    @task
    def load(data):
        return "loaded"

    load(transform(extract()))

with DAG(dag_id="demo_failing_bash", **COMMON):
    BashOperator(
        task_id="run_script",
        bash_command="echo 'starting export'; echo 'ERROR: write failed: disk full on /mnt/staging' >&2; exit 3",
    )

with DAG(dag_id="demo_failing_retry", **COMMON):

    @task(retries=2, retry_delay=timedelta(seconds=20))
    def flaky():
        raise RuntimeError("Demo failure: upstream API returned HTTP 503")

    flaky()

with DAG(dag_id="demo_failing_mapped", **COMMON):

    @task
    def process(item):
        if item >= 2:
            raise RuntimeError(f"Demo failure: item {item} could not be processed")
        return item

    process.expand(item=[1, 2, 3])

with DAG(dag_id="demo_success", **COMMON):

    @task
    def ok():
        return "fine"

    ok()
