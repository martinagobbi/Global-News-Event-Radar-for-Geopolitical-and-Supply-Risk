"""
gdelt_urls.py
Costruisce gli URL per scaricare i file GDELT 2.0.
GDELT pubblica un nuovo file ogni 15 minuti.
Ogni timestamp ha tre file: export (Events), mentions, gkg.
"""

from datetime import datetime, timedelta, timezone


GDELT_BASE_URL = "http://data.gdeltproject.org/gdeltv2" #cartella radice (il server FTP) in cui sono depositati i file
LAST_UPDATE_URL = f"{GDELT_BASE_URL}/lastupdate.txt"

# I tre tipi di file pubblicati ogni 15 minuti
FILE_TYPES = {
    "events": "export.CSV.zip",
    "mentions": "mentions.CSV.zip",
    "gkg": "gkg.csv.zip",
}


def round_to_15min(dt: datetime) -> datetime:
    """
    Arrotonda un datetime al quarto d'ora precedente.
    GDELT pubblica alle :00, :15, :30, :45 di ogni ora.
    """
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def timestamp_to_gdelt_str(dt: datetime) -> str:
    """
    Converte un datetime nel formato stringa usato da GDELT.
    Es: 2024-03-15 14:30:00 -> '20240315143000'
    """
    return dt.strftime("%Y%m%d%H%M%S")


def build_file_url(dt: datetime, file_type: str) -> str:
    """
    Costruisce l'URL di un singolo file GDELT dato timestamp e tipo.

    Args:
        dt:        datetime del file (già arrotondato a 15 min)
        file_type: 'events', 'mentions', o 'gkg'

    Returns:
        URL completo del file .zip
    """
    if file_type not in FILE_TYPES:
        raise ValueError(f"file_type deve essere uno tra: {list(FILE_TYPES.keys())}")

    ts_str = timestamp_to_gdelt_str(dt)
    suffix = FILE_TYPES[file_type]
    return f"{GDELT_BASE_URL}/{ts_str}.{suffix}"


def generate_timestamps_last_n_days(n_days: int, end_dt: datetime = None):
    """
    Genera tutti i timestamp GDELT degli ultimi N giorni.
    Ogni giorno ha 96 slot da 15 minuti = 96 file per tipo.

    Args:
        n_days:  numero di giorni da coprire (es. 30)
        end_dt:  datetime di fine (default: adesso UTC)

    Yields:
        datetime arrotondato a 15 min, dal più vecchio al più recente
    """
    if end_dt is None:
        end_dt = datetime.now(timezone.utc)

    end_dt = round_to_15min(end_dt)
    start_dt = end_dt - timedelta(days=n_days)

    current = start_dt
    while current <= end_dt:
        yield current
        current += timedelta(minutes=15)


def generate_urls_last_n_days(n_days: int, file_types=("events", "gkg")):
    """
    Genera tutti gli URL GDELT degli ultimi N giorni per i tipi richiesti.

    Args:
        n_days:     numero di giorni (es. 30)
        file_types: tupla di tipi da scaricare (default: events + gkg)

    Yields:
        dict con 'timestamp', 'file_type', 'url'
    """
    for dt in generate_timestamps_last_n_days(n_days):
        for ft in file_types:
            yield {
                "timestamp": dt,
                "file_type": ft,
                "url": build_file_url(dt, ft),
            }


def count_urls(n_days: int, file_types=("events", "gkg")) -> int:
    """
    Stima quanti file verranno scaricati — utile per progress bar.
    Formula: n_days * 96 slot/giorno * len(file_types)
    """
    return n_days * 96 * len(file_types)