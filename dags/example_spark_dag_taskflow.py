"""
Example Airflow DAG with Spark Integration (TaskFlow API version)
Same pipeline as example_spark_dag.py, written with @dag / @task decorators
"""

from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# The decorated function body defines the DAG; the function name is not the dag_id
@dag(
    dag_id='example_spark_pipeline_taskflow',
    default_args=default_args,
    description='Example pipeline with Spark jobs (TaskFlow API)',
    start_date=datetime(2026, 2, 16),
    schedule='@daily',  # Run once a day
    catchup=False,  # Don't backfill past runs
    tags=['example', 'spark', 'taskflow'],
)
def example_spark_pipeline_taskflow():

    # Task 1: Simple Python task (replaces PythonOperator)
    @task
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

    # Task 2: Run Spark KMeans clustering job
    # There is no decorator for spark-submit, so the classic operator stays;
    # inside a @dag function it does not need dag=dag
    kmeans_spark_job = SparkSubmitOperator(
        task_id='run_kmeans_clustering',
        application='/opt/spark-apps/kmeans_clustering.py',
        verbose=True,
        conf={
            'spark.driver.memory': '1g',
            'spark.executor.memory': '1g',
        },
    )

    # Task 3: Post-processing task
    @task
    def post_process():
        """Simple post-processing after Spark job"""
        print("Spark job completed successfully!")
        print("Performing post-processing...")
        # Add your post-processing logic here
        return "Processing complete"

    # Task 4: Cleanup or notification task (replaces BashOperator)
    # @task.bash runs the string the function returns as a bash command
    @task.bash
    def cleanup():
        return 'echo "Pipeline completed at $(date)"'

    # Define task dependencies (execution order)
    # Calling a @task function creates the task inside this DAG
    validate_data() >> kmeans_spark_job >> post_process() >> cleanup()


# Calling the @dag function registers the DAG with Airflow
example_spark_pipeline_taskflow()
