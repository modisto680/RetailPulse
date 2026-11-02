# RetailPulse — Spark Structured Streaming (pyspark en mode local)
FROM python:3.11-slim-bookworm

# Java 17 requis par Spark, curl pour télécharger les connecteurs.
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Connecteurs Kafka + driver JDBC PostgreSQL ajoutés directement au classpath
# de pyspark : aucune dépendance au runtime, image déterministe.
ARG JARS_DIR=/usr/local/lib/python3.11/site-packages/pyspark/jars
RUN curl -fsSL -o ${JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
 && curl -fsSL -o ${JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
 && curl -fsSL -o ${JARS_DIR}/kafka-clients-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSL -o ${JARS_DIR}/commons-pool2-2.12.0.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
 && curl -fsSL -o ${JARS_DIR}/postgresql-42.7.3.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

COPY spark_job.py .

RUN useradd --create-home sparkuser \
 && mkdir -p /opt/spark/checkpoint \
 && chown -R sparkuser /opt/spark /app
USER sparkuser

ENTRYPOINT ["python", "spark_job.py"]