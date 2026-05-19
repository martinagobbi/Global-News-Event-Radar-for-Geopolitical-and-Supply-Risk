import argparse
import json
import time
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# IMPORTA IL PRODUCER KAFKA
try:
    from src.ingestion.kafka_producer import push_to_kafka
except ImportError:

    from kafka_producer import push_to_kafka

MASTER_FILE_LIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
DATASET_PATTERNS = {
    "events": ".export.CSV.zip",
    "gkg": ".gkg.csv.zip",
}
POLL_INTERVAL_SECONDS = 15 * 60

BASE_DIR = Path(__file__).resolve().parent.parent.parent # Corretto per arrivare alla root 'risk_bdt'
RAW_ZIP_DIR = BASE_DIR / "data" / "raw" / "zip"
RAW_CSV_DIR = BASE_DIR / "data" / "raw" / "csv"
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

def fetch_master_file_list(session: requests.Session) -> str:
    response = session.get(MASTER_FILE_LIST_URL, timeout=30)
    response.raise_for_status()
    return response.text

def extract_latest_file_url(master_file_text: str, dataset: str) -> str:
    pattern = DATASET_PATTERNS[dataset]
    matching_urls = []
    for line in master_file_text.splitlines():
        parts = line.strip().split()
        if len(parts) < 3:
            continue
        candidate_url = parts[2]
        if candidate_url.endswith(pattern):
            matching_urls.append(candidate_url)
    if not matching_urls:
        raise ValueError(f"Nessun file trovato per dataset={dataset}")
    return matching_urls[-1]

def already_processed(state: dict, dataset: str, file_url: str) -> bool:
    return state.get(dataset) == file_url

def download_file(session: requests.Session, file_url: str) -> bytes:
    response = session.get(file_url, timeout=120)
    response.raise_for_status()
    return response.content

def save_zip_file(file_url: str, content: bytes) -> Path:
    zip_path = RAW_ZIP_DIR / Path(file_url).name
    zip_path.write_bytes(content)
    return zip_path

def extract_zip(zip_path: Path) -> Path:
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        if not members:
            raise ValueError(f"Archivio vuoto: {zip_path}")
        first_member = members[0]
        extracted_path = RAW_CSV_DIR / Path(first_member).name
        extracted_path.write_bytes(zf.read(first_member))
        return extracted_path

def validate_csv(csv_path: Path) -> None:
    sample_df = pd.read_csv(csv_path, sep="\t", header=None, nrows=5, low_memory=False)
    print(f"[OK] CSV valido: {csv_path.name} | sample rows: {len(sample_df)}")

# LOGICA DI INVIO A KAFKA E GESTIONE STATO
def process_latest_file(session: requests.Session, dataset: str) -> Optional[Path]:
    ensure_directories()
    state = load_state()

    master_text = fetch_master_file_list(session)
    latest_file_url = extract_latest_file_url(master_text, dataset)

    if already_processed(state, dataset, latest_file_url):
        print(f"[SKIP] File già processato per {dataset}")
        return None

    print(f"[INFO] Download file: {latest_file_url}")
    content = download_file(session, latest_file_url)
    zip_path = save_zip_file(latest_file_url, content)
    csv_path = extract_zip(zip_path)
    
    validate_csv(csv_path)

    # INVIO A KAFKA 
    print(f"[KAFKA] Caricamento dati in corso...")
    # Carichiamo l'intero file in Pandas (senza header come da standard GDELT)
    df = pd.read_csv(csv_path, sep="\t", header=None, low_memory=False)
    
    # Trasformiamo in lista di dizionari per Kafka
    records = df.to_dict(orient='records')
    
    # Inviamo al topic 
    push_to_kafka("gdelt_raw", records)
    print(f"[OK] Inviati {len(records)} record a Kafka")
    # --------------------

    state[dataset] = latest_file_url
    save_state(state)
    return csv_path

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poller GDELT")
    parser.add_argument("--dataset", choices=["events", "gkg"], default="events")
    parser.add_argument("--loop", action="store_true")
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    with requests.Session() as session:
        if args.loop:
            while True:
                try:
                    process_latest_file(session, args.dataset)
                except Exception as exc:
                    print(f"[ERROR] {exc}")
                time.sleep(POLL_INTERVAL_SECONDS)
        else:
            process_latest_file(session, args.dataset)

if __name__ == "__main__":
    main()