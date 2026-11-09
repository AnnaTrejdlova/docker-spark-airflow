"""
KMeans clustering for Google Cloud Dataproc.

Same clustering as kmeans_clustering.py, but data lives in Cloud Storage (gs://...)
instead of the shared /opt/spark-data folder. The job writes its results back to
Cloud Storage; the Airflow DAG (kmeans_gcp_dag.py) downloads them and draws the plot.

Usage:
    spark-submit kmeans_clustering_gcp.py --input gs://BUCKET/input/artificial_data.csv \
                                          --output gs://BUCKET/output/kmeans [--k 3]
"""

import argparse

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True, help="CSV with columns x,y (gs:// path)")
parser.add_argument("--output", required=True, help="Output folder (gs:// path)")
parser.add_argument("--k", type=int, default=3, help="Number of clusters")
args = parser.parse_args()

# Inicializace SparkSession
spark = SparkSession.builder.appName("KMeansClustering-GCP").getOrCreate()

# Načtení dat z Cloud Storage
df = spark.read.csv(args.input, header=True, inferSchema=True)

# Převod sloupců na vektory pro clustering
vec_assembler = VectorAssembler(inputCols=["x", "y"], outputCol="features")
vec_df = vec_assembler.transform(df)

# KMeans clustering
kmeans = KMeans(k=args.k, seed=1)
model = kmeans.fit(vec_df)
transformed = model.transform(vec_df)

# Kvalita clusteringu (vypíše se do logu jobu v Dataproc / Airflow)
silhouette = ClusteringEvaluator().evaluate(transformed)
print(f"Silhouette score: {silhouette:.4f}")

# Uložení výsledků do Cloud Storage
# coalesce(1) = jeden CSV soubor (data jsou malá, Airflow si je stáhne a vykreslí)
(transformed.select("x", "y", "prediction")
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(f"{args.output}/predictions"))

centers = [(i, float(c[0]), float(c[1])) for i, c in enumerate(model.clusterCenters())]
(spark.createDataFrame(centers, ["cluster", "x", "y"])
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(f"{args.output}/centers"))

print(f"Výsledky byly uloženy do: {args.output}")

# Ukončení SparkSession
spark.stop()
