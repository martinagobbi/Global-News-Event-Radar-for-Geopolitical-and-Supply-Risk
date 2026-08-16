# Processing layer — the resident service AND the Spark driver.
#
# Built on the Spark image because silver -> gold is a PySpark job now
# (spark_gold.py), and this container is what runs it. There is no separate
# pandas implementation any more: keeping two versions of identical semantics
# meant every rule had to be changed in both places and could silently diverge.
#
# SPARK_MASTER decides where the work actually happens, and is the only
# difference between the modes:
#   local[*]                   testing  — Spark runs inside THIS container, so
#                                         no master or worker containers exist
#   spark://spark-master:7077  intended — distributed across the workers
#
# The same image is therefore both the FastAPI service (its CMD) and a valid
# spark-submit target, which is what docker-compose.spark.yml uses for a manual
# batch run.
# bitnamilegacy/, not bitnami/: Bitnami moved their pre-existing images to the
# `bitnamilegacy` namespace, and `bitnami/spark:3.5` no longer resolves at all
# ("failed to resolve source metadata ... not found"). This is the same image,
# re-hosted, so the layout (/opt/bitnami/spark) and the SPARK_MODE convention the
# master/worker services rely on are unchanged.
#
# It is called legacy for a reason and will not get updates. The migration path is
# apache/spark:3.5.x, the official image, which needs three changes this one does
# not: jars live in /opt/spark/jars, there is no SPARK_MODE helper so master and
# worker need explicit start commands, and it runs as UID 185.
FROM bitnamilegacy/spark:3.5

USER root
WORKDIR /app

# fastapi/uvicorn for the service and its two triggers; clickhouse-driver for
# the watermark poll and the retention deletes; pymongo for profiles and tags;
# psycopg for the driver-side publish and the orphan sweep. pyspark itself ships
# with the base image, so it is deliberately not pinned here.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# JDBC drivers: ClickHouse (source) and PostgreSQL (sink). Spark loads them from
# the jars directory, so they are on the classpath for both an in-process
# local[*] session and a submitted job.
ENV IVY_DIR=/opt/bitnami/spark/jars
RUN curl -fsSL -o $IVY_DIR/clickhouse-jdbc.jar \
      https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar && \
    curl -fsSL -o $IVY_DIR/postgresql-jdbc.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

COPY . .

# Port 8001 is the processing layer's own API (/process-all, /process/{user_id},
# /health) — distinct from the serving backend on 8000.
CMD ["uvicorn", "main:app", "--host=0.0.0.0", "--port=8001"]
