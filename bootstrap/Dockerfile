# Bootstrap loader: fills the silver layer in bulk from a folder of GDELT ZIPs.
# The build context is the REPOSITORY ROOT, so the image can reuse the real
# pipeline code rather than duplicating it:
#   * 2-parsing/parser.py            -> the bronze->silver relevance filter
#   * 3-validation_and_storage/      -> the silver schema, sharding and writes
#
# Base image changed from python:3.11-slim to the SAME bitnamilegacy/spark:3.5
# image 4-processing already uses, because bulk_load.py now distributes its work
# with PySpark (see the "Why PySpark" note at the top of bulk_load.py). pyspark
# ships with this image and is deliberately not pinned in requirements.txt, same
# reasoning as 4-processing/Dockerfile. bitnamilegacy/, not bitnami/: bitnami/
# no longer resolves at all — see 4-processing/Dockerfile for the full story.
#
# No JDBC jars are added here, unlike 4-processing. This script writes through
# storage.Storage (clickhouse-driver, a native TCP client), never through Spark's
# JDBC data source, so nothing here ever needs the ClickHouse/PostgreSQL jars
# 4-processing/Dockerfile downloads.
FROM bitnamilegacy/spark:3.5

USER root
WORKDIR /app

# System libraries needed by lxml / Pillow (Newspaper3k dependencies). Matches
# 3-validation_and_storage/Dockerfile; carried over unchanged since bulk_load.py
# still imports newspaper3k directly (see enrichment.py) and this is now a
# different base OS than the python:3.11-slim image that requirement was written
# against.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libxml2-dev libxslt1-dev libjpeg-dev zlib1g-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY 3-validation_and_storage/requirements.txt ./req-validation.txt
RUN pip install --no-cache-dir -r req-validation.txt

# Newspaper3k keyword extraction (article.nlp()) needs the NLTK tokenizer data.
# This MUST match 3-validation_and_storage/Dockerfile: without it, article.nlp()
# raises, enrichment.py swallows the exception, and every mention is stored with
# enriched=1 but an empty article_keywords — a silent failure that looks like a
# successful enrichment and starves the silver->gold keyword filter.
RUN python -m nltk.downloader -d /usr/local/share/nltk_data punkt punkt_tab
ENV NLTK_DATA=/usr/local/share/nltk_data

# A real environment variable, not just the sys.path.insert() calls inside
# bulk_load.py itself: those only affect the driver's own interpreter, but the
# Python worker PROCESSES Spark spawns for mapPartitions() need parser/gdelt/
# validator/storage/enrichment importable in THEIR OWN sys.path too, and this is
# what makes that true without relying on process inheritance being exact. See
# bulk_load.py's _spark() docstring for the full explanation.
#
# APPENDED to ${PYTHONPATH}, never assigned outright. The base image already
# sets PYTHONPATH to /opt/bitnami/spark/python/:...py4j.zip — that is the ONLY
# reason `import pyspark` works at all here, since pyspark is not pip-installed,
# it is a directory under /opt/bitnami/spark that has to be on the path. A plain
# `ENV PYTHONPATH=/app/parsing:/app/validation` REPLACES the base image's value
# instead of adding to it, which breaks `import pyspark` while leaving every
# other symptom looking unrelated. Caught empirically: the first build of this
# Dockerfile did exactly that, and `python3 -c "import pyspark"` failed with
# ModuleNotFoundError even though the same base image's OTHER project service
# (4-processing) imports pyspark from it successfully.
ENV PYTHONPATH=/app/parsing:/app/validation:${PYTHONPATH}

COPY 2-parsing/parser.py            /app/parsing/parser.py
COPY 3-validation_and_storage/      /app/validation/
COPY bootstrap/bulk_load.py         /app/bulk_load.py

CMD ["python", "bulk_load.py"]
