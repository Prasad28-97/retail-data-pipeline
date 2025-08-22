
# # AIRFLOW DAGS SUBMITTING JOBS TO DATAPROC SERVERLESS


# # workflows/pyspark_dag.py
# from datetime import timedelta
# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# PROJECT_ID = "thematic-land-467710-p8"
# REGION = "us-east1"
# COMPOSER_BUCKET = "us-central1-demo-composer-603b77d1-bucket"

# GCS_JOB_FILE_1 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/retailerMysqlToLanding.py"
# GCS_JOB_FILE_2 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/supplierMysqlToLanding.py"
# GCS_JOB_FILE_3 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/customerReviews_API.py"

# RUNTIME_CONFIG = {
#     "version": "1.1",
#     "properties": {
#         "spark.executor.instances": "2",
#         "spark.executor.cores": "4",
#         "spark.executor.memory": "4g",
#         "spark.driver.cores": "4",
#         "spark.driver.memory": "4g",
#     },
# }

# default_args = {
#     "owner": "Prasad",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="pyspark_dag",
#     description="PySpark ingestion via Dataproc Serverless",
#     default_args=default_args,
#     start_date=days_ago(1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:

#     def make_batch(main_py_uri: str) -> dict:
#         return {
#             "pyspark_batch": {"main_python_file_uri": main_py_uri},
#             "runtime_config": RUNTIME_CONFIG,
#         }

#     task1 = DataprocCreateBatchOperator(
#         task_id="pyspark_task_1",
#         project_id=PROJECT_ID,
#         region=REGION,
#         batch_id="pyspark-task-1-{{ ds_nodash }}",
#         batch=make_batch(GCS_JOB_FILE_1),
#     )

#     task2 = DataprocCreateBatchOperator(
#         task_id="pyspark_task_2",
#         project_id=PROJECT_ID,
#         region=REGION,
#         batch_id="pyspark-task-2-{{ ds_nodash }}",
#         batch=make_batch(GCS_JOB_FILE_2),
#     )

#     task3 = DataprocCreateBatchOperator(
#         task_id="pyspark_task_3",
#         project_id=PROJECT_ID,
#         region=REGION,
#         batch_id="pyspark-task-3-{{ ds_nodash }}",
#         batch=make_batch(GCS_JOB_FILE_3),
#     )

#     task1 >> task2 >> task3







#===============================================================================================================================

# AIRFLOW DAGS SUBMITTING JOBS TO DATAPROC STANDARD

# import all modules
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)

# define the variables
PROJECT_ID = "thematic-land-467710-p8"
REGION = "us-east1"
CLUSTER_NAME = "my-demo-cluster"
COMPOSER_BUCKET = "us-central1-demo-composer-603b77d1-bucket"

GCS_JOB_FILE_1 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/retailerMysqlToLanding.py"
PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_1},
}

GCS_JOB_FILE_2 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/supplierMysqlToLanding.py"
PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_2},
}

GCS_JOB_FILE_3 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/customerReviews_API.py"
PYSPARK_JOB_3 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_3},
}


ARGS = {
    "owner": "Prasad",
    "start_date": None,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# define the dag
with DAG(
    dag_id="pyspark_dag",
    schedule_interval=None,
    description="DAG to start a Dataproc cluster, run PySpark jobs, and stop the cluster",
    default_args=ARGS,
    tags=["pyspark", "dataproc", "etl", "marvel"]
) as dag:
    
    # define the Tasks
    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id="pyspark_task_1", 
        job=PYSPARK_JOB_1, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id="pyspark_task_2", 
        job=PYSPARK_JOB_2, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    pyspark_task_3 = DataprocSubmitJobOperator(
        task_id="pyspark_task_3", 
        job=PYSPARK_JOB_3, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

# define the task dependencies
start_cluster >> [pyspark_task_1, pyspark_task_2, pyspark_task_3] >> stop_cluster



#===============================================================================================================================

# AIRFLOW DAGS SUBMITTING BEAM JOBS TO DATAFLOW

# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
# from airflow.providers.apache.beam.hooks.beam import BeamRunnerType

# # Default args
# ARGS = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
# }

# # GCS paths to your scripts
# GCS_JOB_FILE_1 = "gs://us-central1-training-12345-bucket/data/INGESTION/retailerMysqlToLanding.py"
# GCS_JOB_FILE_2 = "gs://us-central1-training-12345-bucket/data/INGESTION/supplierMysqlToLanding.py"
# GCS_JOB_FILE_3 = "gs://us-central1-training-12345-bucket/data/INGESTION/customerReviews_API.py"

# # Common configs
# LOCATION = "us-central1"
# PY_REQUIREMENTS = ["apache-beam[gcp]==2.59.0"]

# with DAG(
#     dag_id="pyspark_dag",
#     schedule_interval="0 5 * * *",
#     description="DAG to run multiple Apache Beam jobs on Dataflow",
#     default_args=ARGS,
#     start_date=days_ago(1),
#     catchup=False,
#     tags=["beam", "dataflow", "etl", "alpha team"],
# ) as dag:

#     retailer_job = BeamRunPythonPipelineOperator(
#         task_id="retailer_job",
#         runner=BeamRunnerType.DataflowRunner,
#         py_file=GCS_JOB_FILE_1,
#         py_options=[],
#         pipeline_options={},
#         py_requirements=PY_REQUIREMENTS,
#         py_interpreter="python3",
#         py_system_site_packages=False,
#         dataflow_config={
#             "location": LOCATION,
#             "job_name": "retailer_job_{{ ds_nodash }}",
#         },
#     )

#     supplier_job = BeamRunPythonPipelineOperator(
#         task_id="supplier_job",
#         runner=BeamRunnerType.DataflowRunner,
#         py_file=GCS_JOB_FILE_2,
#         py_options=[],
#         pipeline_options={},
#         py_requirements=PY_REQUIREMENTS,
#         py_interpreter="python3",
#         py_system_site_packages=False,
#         dataflow_config={
#             "location": LOCATION,
#             "job_name": "supplier_job_{{ ds_nodash }}",
#         },
#     )

#     customer_reviews_job = BeamRunPythonPipelineOperator(
#         task_id="customer_reviews_job",
#         runner=BeamRunnerType.DataflowRunner,
#         py_file=GCS_JOB_FILE_3,
#         py_options=[],
#         pipeline_options={},
#         py_requirements=PY_REQUIREMENTS,
#         py_interpreter="python3",
#         py_system_site_packages=False,
#         dataflow_config={
#             "location": LOCATION,
#             "job_name": "customer_reviews_job_{{ ds_nodash }}",
#         },
#     )

#     # Run jobs sequentially (can change to parallel if required)
#     retailer_job >> supplier_job >> customer_reviews_job
