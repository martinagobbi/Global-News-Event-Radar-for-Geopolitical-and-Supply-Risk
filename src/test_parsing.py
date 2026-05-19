#da eliminare una volta che il progetto sarà concluso e si passerà a docker
# Test di parsing dei file GDELT
from pathlib import Path
import pandas as pd

# 1. Definiamo i nomi delle colonne principali (Schema)
# GDELT non ha header, quindi inventiamo una lista con i nomi ufficiali delle colonne che ci servono
EVENT_COLUMNS = {
    0: "GlobalEventID",
    1: "Day",
    26: "EventCode",
    30: "GoldsteinScale",
    34: "NumMentions",
    40: "ActionGeo_CountryCode",
    48: "ActionGeo_Lat",
    49: "ActionGeo_Long"
}

def parsing_grezzo(csv_input_path):
    # 2. Leggiamo il file locale che il tuo poller ha scaricato nei giorni scorsi
    df = pd.read_csv(csv_input_path, sep="\t", header=None, low_memory=False)
    
    # 3. Rinominiamo solo le colonne che ci interessano e buttiamo le altre
    df = df.rename(columns=EVENT_COLUMNS)
    df = df[list(EVENT_COLUMNS.values())]
    
    # 4. Pulizia: eliminiamo le righe dove mancano le coordinate geografiche
    df = df.dropna(subset=["ActionGeo_Lat", "ActionGeo_Long"])
    
    # 5. Convertiamo la data da intero (es: 20260518) a stringa data standard
    df["Day"] = pd.to_datetime(df["Day"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    
    return df

# Sostituisci questo nome con un file .CSV vero che hai dentro data/raw/csv/!
file_test = Path("src/data/raw/csv/20260514083000.export.CSV") 

if file_test.exists():
    df_pulito = parsing_grezzo(file_test)
    print("Ecco come apparirà il dato pulito che manderemo a Spark:")
    print(df_pulito.head())
else:
    print(f"Inserisci un nome di file valido. Al momento in csv/ hai questi file: {list(Path('data/raw/csv').glob('*.CSV'))}")