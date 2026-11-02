"""
Example Airflow DAG with Spark Integration
This DAG demonstrates how to orchestrate Spark jobs using Airflow
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id='example_spark_pipeline',
    default_args=default_args,
    description='Example pipeline with Spark jobs',
    schedule='@daily',  # Run once a day (changed from schedule_interval in Airflow 3.x)
    catchup=False,  # Don't backfill past runs
    tags=['example', 'spark'],
)


# Task 1: Simple Python task
def validate_data():
    """Validate that input data exists"""
    import os
    data_file = '/opt/spark-data/artificial_data.csv'
    if os.path.exists(data_file):
        print(f"✓ Data file found: {data_file}")
        return True
    else:
        print(f"✗ Data file not found: {data_file}")
        raise FileNotFoundError(f"Data file {data_file} does not exist")


validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)


# Task 2: Run Spark KMeans clustering job
kmeans_spark_job = SparkSubmitOperator(
    task_id='run_kmeans_clustering',
    application='/opt/spark-apps/kmeans_clustering.py',
    verbose=True,
    conf={
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)


# Task 3: Post-processing task
def post_process():
    """Simple post-processing after Spark job"""
    print("Spark job completed successfully!")
    print("Performing post-processing...")
    # Add your post-processing logic here
    return "Processing complete"


post_process_task = PythonOperator(
    task_id='post_process',
    python_callable=post_process,
    dag=dag,
)


# Task 4: Cleanup or notification task
cleanup_task = BashOperator(
    task_id='cleanup',
    bash_command='echo "Pipeline completed at $(date)"',
    dag=dag,
)


# Define task dependencies (execution order)
validate_task >> kmeans_spark_job >> post_process_task >> cleanup_task
