FROM python:3.11-slim

WORKDIR /app

# System libraries needed by lxml / Pillow (Newspaper3k dependencies).
RUN apt-get update && apt-get install -y --no-install-recommends \
        libxml2-dev libxslt1-dev libjpeg-dev zlib1g-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Newspaper3k keyword extraction (article.nlp()) needs the NLTK tokenizer data.
# Downloaded at build time so the container works without fetching it at runtime.
RUN python -m nltk.downloader -d /usr/local/share/nltk_data punkt punkt_tab
ENV NLTK_DATA=/usr/local/share/nltk_data

COPY . .

CMD ["python", "main.py"]
