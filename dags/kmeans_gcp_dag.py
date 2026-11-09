"""
KMeans on Google Cloud Dataproc, orchestrated by the local Airflow.

Airflow keeps running in Docker on your machine; only the Spark job runs in Google Cloud:
  1. check that the GCP settings and the local CSV exist
  2. create the Cloud Storage bucket (skipped if it already exists)
  3. upload the PySpark script and the CSV to the bucket
  4. create a small Dataproc cluster
  5. run kmeans_clustering_gcp.py on it
  6. delete the cluster (always, even if the job failed, so it does not cost money)
  7. download the results and save the plot to data/output_gcp.png

Settings come from environment variables set in docker-compose.yml (from the .env file):
GCP_PROJECT_ID, GCP_REGION, GCP_BUCKET.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.sdk import dag, task

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
REGION = os.environ.get("GCP_REGION", "europe-west1")
BUCKET = os.environ.get("GCP_BUCKET", "")
CLUSTER_NAME = "kmeans-cluster"

LOCAL_SCRIPT = "/opt/spark-apps/kmeans_clustering_gcp.py"
LOCAL_DATA = "/opt/spark-data/artificial_data.csv"
LOCAL_PLOT = "/opt/spark-data/output_gcp.png"

GCS_SCRIPT = "apps/kmeans_clustering_gcp.py"
GCS_DATA = "input/artificial_data.csv"
GCS_OUTPUT = "output/kmeans"

# Single-node cluster (master only, no workers): 4 vCPUs fits the free-trial CPU quota.
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "software_config": {
        "image_version": "2.2-debian12",  # Spark 3.5, like the local cluster
        "properties": {"dataproc:dataproc.allow.zero.workers": "true"},
    },
    # Image 2.2+ defaults to internal IPs only, which needs Private Google Access on the subnet
    "gce_cluster_config": {"internal_ip_only": False},
    # Safety net: Google deletes the cluster after 30 idle minutes if delete_cluster never ran
    "lifecycle_config": {"idle_delete_ttl": {"seconds": 1800}},
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET}/{GCS_SCRIPT}",
        "args": [
            "--input", f"gs://{BUCKET}/{GCS_DATA}",
            "--output", f"gs://{BUCKET}/{GCS_OUTPUT}",
            "--k", "3",
        ],
    },
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # a retry would create another cluster; rerun the DAG instead
}


@dag(
    dag_id='kmeans_gcp_dataproc',
    default_args=default_args,
    description='KMeans clustering on Google Cloud Dataproc',
    start_date=datetime(2026, 2, 16),
    schedule=None,  # trigger manually: every run starts a paid cluster
    catchup=False,
    max_active_runs=1,  # runs share one cluster name
    dagrun_timeout=timedelta(hours=1),
    tags=['example', 'spark', 'gcp', 'dataproc'],
)
def kmeans_gcp_dataproc():

    @task
    def check_config():
        """Fail early with a clear message instead of deep inside a Google API call"""
        missing = [name for name in ("GCP_PROJECT_ID", "GCP_BUCKET") if not os.environ.get(name)]
        if missing:
            raise ValueError(f"Set {', '.join(missing)} in the .env file and recreate the airflow container")
        creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
        if not os.path.isfile(creds):
            raise FileNotFoundError(f"Google credentials not found at {creds!r} (see README, section GCP)")
        if not os.path.isfile(LOCAL_DATA):
            raise FileNotFoundError(f"Data file {LOCAL_DATA} does not exist")
        print(f"✓ Project {PROJECT_ID}, region {REGION}, bucket gs://{BUCKET}")

    create_bucket = GCSCreateBucketOperator(
        task_id='create_bucket',
        bucket_name=BUCKET,
        project_id=PROJECT_ID,
        location=REGION,
        storage_class='STANDARD',  # the default MULTI_REGIONAL is not allowed in a single region
    )

    upload_script = LocalFilesystemToGCSOperator(
        task_id='upload_script',
        src=LOCAL_SCRIPT,
        dst=GCS_SCRIPT,
        bucket=BUCKET,
    )

    upload_data = LocalFilesystemToGCSOperator(
        task_id='upload_data',
        src=LOCAL_DATA,
        dst=GCS_DATA,
        bucket=BUCKET,
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
        use_if_exists=True,  # reuse a cluster left over from an interrupted run
    )

    run_kmeans = DataprocSubmitJobOperator(
        task_id='run_kmeans_clustering',
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule='all_done',  # run even when the job failed
    )

    @task
    def plot_results():
        """Download the job output from Cloud Storage and draw it like kmeans_clustering.py"""
        import io

        import matplotlib
        matplotlib.use("Agg")  # no display inside the container
        import matplotlib.pyplot as plt
        import pandas as pd
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        hook = GCSHook()

        def read_csv(folder):
            # Spark writes a folder with one part-*.csv file (plus a _SUCCESS marker)
            files = [f for f in hook.list(BUCKET, prefix=f"{GCS_OUTPUT}/{folder}/") if f.endswith(".csv")]
            if not files:
                raise FileNotFoundError(f"No CSV in gs://{BUCKET}/{GCS_OUTPUT}/{folder}/")
            return pd.read_csv(io.BytesIO(hook.download(BUCKET, files[0])))

        predictions = read_csv("predictions")
        centers = read_csv("centers")

        plt.figure(figsize=(8, 6))
        for cluster_id, points in predictions.groupby("prediction"):
            plt.scatter(points["x"], points["y"], label=f"Cluster {cluster_id}")
        plt.scatter(centers["x"], centers["y"], color="black", marker="x", s=150, label="Centers")
        plt.title("KMeans Clustering (Google Cloud Dataproc)")
        plt.xlabel("x")
        plt.ylabel("y")
        plt.legend()
        plt.savefig(LOCAL_PLOT)
        print(f"Výsledek byl uložen do: {LOCAL_PLOT}")

    check = check_config()
    check >> create_bucket >> [upload_script, upload_data] >> create_cluster
    create_cluster >> run_kmeans >> delete_cluster
    run_kmeans >> plot_results()


kmeans_gcp_dataproc()
