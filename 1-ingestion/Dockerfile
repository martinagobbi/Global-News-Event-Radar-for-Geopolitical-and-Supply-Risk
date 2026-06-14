FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# MODE=poller (default): avvia il loop di polling ogni 15 minuti
# MODE=backfill: scarica lo storico degli ultimi N giorni (passa --days e --workers come CMD)
ENV MODE=poller

ENTRYPOINT ["sh", "-c", "\
  if [ \"$MODE\" = 'backfill' ]; then \
    python -m src.ingestion.backfill $0 $@; \
  else \
    python main.py --loop; \
  fi"]
