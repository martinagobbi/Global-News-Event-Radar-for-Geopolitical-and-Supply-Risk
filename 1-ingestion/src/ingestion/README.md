# Ingestion Layer - GDELT Data Pipeline

Questo modulo si occupa della fase iniziale di **Data Ingestion** per l'architettura Big Data del progetto. Il suo scopo principale è monitorare, scaricare, validare e trasmettere in tempo reale i flussi di dati globali estratti da **GDELT v2** (Events e GKG) verso il cluster Apache Kafka.

## 📁 Struttura del Modulo

La cartella `src/ingestion/` è organizzata secondo criteri di modularità e separazione delle responsabilità:

* `gdelt_urls.py`: Gestisce l'interazione con il server GDELT, scansionando la `masterfilelist.txt` per identificare l'URL del file più recente.
* `poller.py`: L'orchestratore principale. Coordina il download dei pacchetti compressi, l'estrazione locale, la validazione strutturale e il passaggio dei record al sistema di messaggistica.
* `kafka_producer.py`: Il client Kafka dedicato alla serializzazione dei record in formato JSON e al loro invio (`push`) verso i topic dedicati.
* `backfill.py`: Script di utilità per il recupero storico dei dati (gestione dei dati pregressi).

---

## ⚙️ Logica di Funzionamento (Workflow)

Ogni volta che viene eseguito il `poller.py`, l'architettura esegue automaticamente i seguenti passaggi:

1. **Controllo dello Stato**: Viene letto il file `state/last_seen.json`. Se l'URL dell'ultimo file presente su GDELT coincide con quello registrato, il processo va in `SKIP` per evitare duplicazioni e spreco di banda.
2. **Download e Decompressione**: I file `.zip` vengono scaricati nella cartella temporanea di staging `data/raw/zip/` e scompattati in formato `.csv` (tab-separated) in `data/raw/csv/`.
3. **Validazione Integrità**: Viene eseguito un check strutturale veloce (`validate_csv`) tramite Pandas per assicurarsi che il file non sia corrotto.
4. **Conversione e Handoff**: Il file CSV grezzo viene convertito in una lista di record JSON-like e trasmesso alla funzione `push_to_kafka`.
5. **Aggiornamento Stato**: Solo ad invio completato, lo stato locale viene aggiornato per garantire la semantica di distribuzione *at-least-once*.

---

## 🧠 Logica del Parsing (Verso il Bronze Layer)

I file nativi di GDELT presentano delle sfide strutturali complesse che richiedono una precisa **logica di parsing** prima che i dati possano essere utilizzati dai modelli di processing o inseriti nel data lake. Questa logica fa da ponte tra i dati grezzi ricevuti e la successiva fase di analisi.

### 1. Assenza di Header (Mappatura dei Campi)
I file CSV di GDELT **non contengono i nomi delle colonne**. Il parser si occupa di applicare uno schema predefinito (es. `EVENT_COLUMNS` ricavato dalla documentazione ufficiale di GDELT v2). 
* *Esempio*: La colonna posizionale `0` viene mappata come `GlobalEventID`, la colonna `1` come `Day`, e così via.

### 2. Conversione dei Tipi di Dato (Data Typing)
Il file grezzo tratta nativamente ogni campo come stringa o intero generico. Il parsing impone il corretto data type:
* **Date**: Stringhe numeriche come `20260517` vengono convertite in oggetti Timestamp/Date standard (`YYYY-MM-DD`).
* **Coordinate Geografiche**: I campi relativi a Latitudine e Longitudine dell'evento vengono castati esplicitamente in `Float` per permettere query geospaziali.

### 3. Filtraggio e Riduzione del Rumore (Dimensionality Reduction)
Un singolo file di eventi GDELT contiene oltre 50 colonne, molte delle quali contengono informazioni ridondanti o non utili ai fini del calcolo del nostro indice di rischio. La logica di parsing isola e trattiene solo le feature core:
* Identificativi dell'evento (`GlobalEventID`).
* Attori coinvolti (`Actor1Code`, `Actor2Code`).
* Codice dell'azione e impatto (`EventCode`, `GoldsteinScale`, `NumMentions`).
* Informazioni geografiche (`ActionGeo_Lat`, `ActionGeo_Long`, `ActionGeo_CountryCode`).

### 4. Normalizzazione Textual-Mining (Specifico per GKG)
Per il dataset **GKG (Global Knowledge Graph)**, il parser affronta campi complessi e semi-strutturati (es. le colonne `Themes` o `Locations`), che contengono liste di elementi separati da punti e virgola (`;`). Il parser implementa funzioni di string-splitting e flat-mapping per rendere queste informazioni indicizzabili.

---

## 🚀 Modalità di Esecuzione

### Requisiti Preliminari
Assicurarsi di aver attivato l'ambiente virtuale e installato le dipendenze:
```bash
source .venv/bin/activate
pip install -r requirements.txt