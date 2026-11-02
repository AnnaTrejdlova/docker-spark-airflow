```bash
docker build -t annatr/apache-spark:3.5.7-java11-python3.13 .
docker-compose up -d
```

```bash
docker exec -it spark-master bash
cd /opt/spark/bin
./spark-submit --master spark://spark-master:7077 "/opt/spark-apps/kmeans_clustering_hadoop.py"
```
