from airflow import DAG
from airflow.utils.dates import days_ago, datetime
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
import json
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, \
    DataprocDeleteClusterOperator, DataprocSubmitJobOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False
}
CLUSTER_NAME = 'stack-data-pipeline'
REGION = 'us-central1'
PROJECT_ID = 'stack-data-pipeline-gcp'
CODE_BUCKET_NAME = 'stack-data-pipeline-gcp-mg-combustiveis-brasil-pyspark-code'
PYSPARK_FILE = 'main.py'

CLUSTER_CONFIG = {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
        },
    }

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{CODE_BUCKET_NAME}/{PYSPARK_FILE}",
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.27.1.jar"]    
    }    
}

with DAG(
        dag_id="dag_olist_customer",
        default_args=default_args,
        description="Dag de carga de dados customer camada curated",
        #start_date=datetime(2004, 1, 1),
        #schedule_interval="0 0 1 */6 *",
        start_date=days_ago(2),
        schedule_interval=None,
        tags=["olist_customer"],
        max_active_runs=1
        # catchup=False
) as dag:

    start_dag = DummyOperator(task_id="start_dag")

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME
        )

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
        )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )
    
    fim_dag = DummyOperator(task_id="fim_dag")
    
    start_dag >>  create_cluster >> submit_job >> delete_cluster >> fim_dag