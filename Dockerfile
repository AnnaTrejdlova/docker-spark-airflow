# builder step used to download and configure spark environment
# Debian 13 ships Python 3.13 (Ubuntu noble only has 3.12)
FROM debian:13 AS builder

# Add Dependencies for PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl vim wget ssh net-tools ca-certificates

# Add Eclipse Temurin repository for OpenJDK 11
RUN mkdir -p /etc/apt/keyrings \
    && wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | tee /etc/apt/keyrings/adoptium.asc \
    && echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(. /etc/os-release && echo $VERSION_CODENAME) main" | tee /etc/apt/sources.list.d/adoptium.list

RUN apt-get update && \
    apt-get install -y --no-install-recommends temurin-11-jre \
    && ln -s /usr/lib/jvm/temurin-11-jre-$(dpkg --print-architecture) /usr/lib/jvm/java-11

ENV JAVA_HOME=/usr/lib/jvm/java-11
RUN apt-get install -y --no-install-recommends python3.13 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy

RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3.13)" 1

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV SPARK_VERSION=3.5.7 \
HADOOP_VERSION=3 \
SPARK_HOME=/opt/spark \
PYSPARK_PYTHON=python3.13 \
PYTHONHASHSEED=1

# Download and uncompress spark from the apache archive
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz


# Apache spark environment
FROM builder AS apache-spark

WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077" \
SPARK_WORKLOAD="master"

EXPOSE 8080 7077 6066

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

# Create a non-root user to run Spark
RUN groupadd -r spark && useradd -r -g spark spark && \
    chown -R spark:spark /opt/spark

COPY start-spark.sh /
RUN chmod +x /start-spark.sh

USER spark

CMD ["/bin/bash", "/start-spark.sh"]
