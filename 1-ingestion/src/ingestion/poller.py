import argparse
import json
import time
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests

# Cambiato all'URL dei 15 minuti, specifico per lo streaming real-time
LAST_15MIN_URL = "http://data.gdeltproject.org/gdeltv2/last15minutes.txt"

POLL_INTERVAL_SECONDS = 15 * 60

BASE_DIR = Path(__file__).resolve().parent.parent.parent
# In Docker, /data è il volume condiviso (shared_data). In locale, si usa data/ relativo alla root.
DATA_DIR = Path("/data") if Path("/data").exists() else BASE_DIR / "data"
RAW_ZIP_DIR = DATA_DIR / "raw" / "zip"
RAW_CSV_DIR = DATA_DIR / "raw" / "csv"
STATE_DIR = BASE_DIR / "state"
STATE_FILE = STATE_DIR / "last_seen.json"

def ensure_directories() -> None:
    RAW_ZIP_DIR.mkdir(parents=True, exist_ok=True)
    RAW_CSV_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    with STATE_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state: dict) -> None:
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

def fetch_latest_urls(session: requests.Session) -> Dict[str, str]:
    """
    Legge il file temporaneo di GDELT ed estrae gli ultimi URL di Events e Mentions.
    Include un meccanismo di retry in caso di 404 temporaneo del server.
    """
    retries = 3
    delay = 5  # secondi da aspettare tra i tentativi
    
    for attempt in range(1, retries + 1):
        try:
            response = session.get(LAST_15MIN_URL, timeout=30)
            response.raise_for_status()
            
            urls = {}
            for line in response.text.splitlines():
                parts = line.strip().split()
                if len(parts) < 3:
                    continue
                candidate_url = parts[2]
                
                if candidate_url.endswith(".export.CSV.zip"):
                    urls["events"] = candidate_url
                elif candidate_url.endswith(".mentions.CSV.zip"):
                    urls["mentions"] = candidate_url
                    
            return urls
            
        except requests.exceptions.HTTPError as http_err:
            # Se è un 404 ed abbiamo ancora tentativi, aspettiamo e riproviamo
            if response.status_code == 404 and attempt < retries:
                print(f"[WARNING] GDELT ha risposto con 404 (tentativo {attempt}/{retries}). Il file si sta aggiornando. Riprovo tra {delay} secondi...")
                time.sleep(delay)
                continue
            raise http_err  # Se i tentativi sono finiti o è un altro errore, fallisci
        except requests.exceptions.RequestException as req_err:
            if attempt < retries:
                time.sleep(delay)
                continue
            raise req_err
            
    return {}

def download_and_extract(session: requests.Session, file_url: str) -> tuple[Path, Path]:
    """Scarica, salva il file zip ed estrae il CSV. Restituisce (csv_path, zip_path)."""
    response = session.get(file_url, timeout=120)
    response.raise_for_status()
    content = response.content

    # Salva ZIP
    zip_path = RAW_ZIP_DIR / Path(file_url).name
    zip_path.write_bytes(content)

    # Estrae CSV
    with zipfile.ZipFile(zip_path, "r") as zf:
        first_member = zf.namelist()[0]
        extracted_path = RAW_CSV_DIR / Path(first_member).name
        extracted_path.write_bytes(zf.read(first_member))
        return extracted_path, zip_path

def validate_and_cleanup(csv_path: Path, zip_path: Path) -> None:
    """Valida il CSV, lo scrive sul volume condiviso per il parsing, poi rimuove lo ZIP."""
    df = pd.read_csv(csv_path, sep="\t", header=None, low_memory=False)
    print(f"[OK] CSV valido: {csv_path.name} | Righe rilevate: {len(df)}")

    # Il CSV rimane in RAW_CSV_DIR (/data/raw/csv/) dove il parsing lo leggerà.
    # Il parsing è responsabile di cancellarlo dopo averlo processato.
    zip_path.unlink(missing_ok=True)
    print(f"[CLEANUP] Rimosso ZIP raw: {zip_path.name}")

def process_pipeline(session: requests.Session) -> None:
    """Esegue il ciclo completo per caricare sia gli Events che le Mentions"""
    ensure_directories()
    state = load_state()

    # 1. Recupera gli ultimi URL disponibili
    latest_urls = fetch_latest_urls(session)
    
    if not latest_urls.get("events") or not latest_urls.get("mentions"):
        print("[WARNING] Impossibile trovare gli URL di Events o Mentions nel file di controllo.")
        return

    # 2. Gestione degli EVENTI
    event_url = latest_urls["events"]
    if state.get("events") == event_url:
        print("[SKIP] Tabella Events già aggiornata all'ultimo rilascio.")
    else:
        print(f"[INFO] Nuovo file Events rilevato: {Path(event_url).name}")
        csv_events, zip_events = download_and_extract(session, event_url)
        validate_and_cleanup(csv_events, zip_events)
        state["events"] = event_url

    # 3. Gestione delle MENZIONI
    mention_url = latest_urls["mentions"]
    if state.get("mentions") == mention_url:
        print("[SKIP] Tabella Mentions già aggiornata all'ultimo rilascio.")
    else:
        print(f"[INFO] Nuovo file Mentions rilevato: {Path(mention_url).name}")
        csv_mentions, zip_mentions = download_and_extract(session, mention_url)
        validate_and_cleanup(csv_mentions, zip_mentions)
        state["mentions"] = mention_url

    # 4. Salva lo stato aggiornato
    save_state(state)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poller GDELT (Events & Mentions)")
    parser.add_argument("--loop", action="store_true", help="Resta in ascolto ogni 15 minuti")
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    with requests.Session() as session:
        if args.loop:
            print("[START] Poller avviato in modalità continua (loop = 15 min)...")
            while True:
                try:
                    process_pipeline(session)
                except Exception as exc:
                    print(f"[ERROR] Errore nel ciclo di esecuzione: {exc}")
                time.sleep(POLL_INTERVAL_SECONDS)
        else:
            print("[START] Poller avviato per esecuzione singola...")
            process_pipeline(session)

if __name__ == "__main__":
    main()