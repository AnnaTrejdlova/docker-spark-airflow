```bash
docker build -t annatr/apache-spark:3.5.7-java11-python3.13 .
docker-compose up -d
```

```bash
docker exec -it spark-master bash
cd /opt/spark/bin
./spark-submit --master spark://spark-master:7077 "/opt/spark-apps/kmeans_clustering_hadoop.py"
```

## Airflow
UI: http://localhost:8085, login `admin` / `admin` (set by `entrypoint.sh` on every start).

## KMeans on Google Cloud (Dataproc)
DAG `kmeans_gcp_dataproc` (`dags/kmeans_gcp_dag.py`): Airflow stays in Docker on your machine,
the Spark job (`apps/kmeans_clustering_gcp.py`) runs on a temporary Dataproc cluster.
The DAG uploads the script and `data/artificial_data.csv` to Cloud Storage, creates the cluster,
runs the job, deletes the cluster and saves the plot to `data/output_gcp.png`.

One-time setup (gcloud CLI: `winget install Google.CloudSDK`):

1. Create a project with billing enabled (the free trial is enough) and enable the APIs:
   ```bash
   gcloud auth login
   gcloud config set project MY_PROJECT_ID
   gcloud services enable dataproc.googleapis.com compute.googleapis.com storage.googleapis.com
   ```
2. Let the cluster VMs (Compute Engine default service account) act as Dataproc workers:
   ```bash
   gcloud projects add-iam-policy-binding MY_PROJECT_ID --member="serviceAccount:$(gcloud projects describe MY_PROJECT_ID --format='value(projectNumber)')-compute@developer.gserviceaccount.com" --role=roles/dataproc.worker
   ```
3. Credentials for Airflow: log in and copy the file into `gcp/credentials.json` (ignored by git).
   A service account key JSON works too, saved under the same name.
   ```bash
   gcloud auth application-default login
   gcloud auth application-default set-quota-project MY_PROJECT_ID
   mkdir -p gcp && cp "$APPDATA/gcloud/application_default_credentials.json" gcp/credentials.json
   ```
4. Copy `.env.example` to `.env`, fill in the project ID and a bucket name, then recreate Airflow:
   ```bash
   docker compose up -d --build airflow
   ```
5. Trigger `kmeans_gcp_dataproc` in the Airflow UI. It has no schedule, because every run starts a paid cluster.
   A run takes about 5 minutes and costs a few cents. Job logs are in the `run_kmeans_clustering` task log
   and in the Cloud console under Dataproc → Jobs.

If a run is interrupted, check that no cluster is left running:
`gcloud dataproc clusters list --region=europe-west1`. The cluster also deletes itself after 30 idle minutes.
