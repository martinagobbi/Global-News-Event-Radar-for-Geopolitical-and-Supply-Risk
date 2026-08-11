# Bootstrap loader: fills the silver layer in bulk from a folder of GDELT ZIPs.
# The build context is the REPOSITORY ROOT, so the image can reuse the real
# pipeline code rather than duplicating it:
#   * 2-parsing/parser.py            -> the bronze->silver relevance filter
#   * 3-validation_and_storage/      -> the silver schema, sharding and writes
FROM python:3.11-slim

WORKDIR /app

COPY 3-validation_and_storage/requirements.txt ./req-validation.txt
RUN pip install --no-cache-dir -r req-validation.txt

# Newspaper3k keyword extraction (article.nlp()) needs the NLTK tokenizer data.
# This MUST match 3-validation_and_storage/Dockerfile: without it, article.nlp()
# raises, enrichment.py swallows the exception, and every mention is stored with
# enriched=1 but an empty article_keywords — a silent failure that looks like a
# successful enrichment and starves the silver->gold keyword filter.
RUN python -m nltk.downloader -d /usr/local/share/nltk_data punkt punkt_tab
ENV NLTK_DATA=/usr/local/share/nltk_data

COPY 2-parsing/parser.py            /app/parsing/parser.py
COPY 3-validation_and_storage/      /app/validation/
COPY bootstrap/bulk_load.py         /app/bulk_load.py

CMD ["python", "bulk_load.py"]
