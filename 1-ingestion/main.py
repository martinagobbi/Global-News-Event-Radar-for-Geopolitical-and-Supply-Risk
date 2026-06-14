#!/usr/bin/env python
"""
Ingestion layer: entry point per il Docker container.
Lancia il poller GDELT in modalità loop (ogni 15 minuti) o in modalità backfill.
La modalità è controllata dalla variabile d'ambiente MODE (default: poller).
"""

from src.ingestion.poller import main

if __name__ == "__main__":
    main()
