# Real-Time Risk Analytics Platform (GDELT Pipeline)

Un'architettura Big Data end-to-end per il monitoraggio, l'ingestione, il processing e la visualizzazione in tempo reale di metriche di rischio globali basate sui dataset **GDELT v2 (Global Data on Events, Location, and Tone)**.

---

## 🏛️ Architettura del Sistema

Il sistema è strutturato come una pipeline di data processing distribuita ed è suddiviso nei seguenti macro-componenti:

1. **Ingestion Layer (Python Poller)**: Monitora costantemente i server GDELT, effettua il download dei flussi in tempo reale (aggiornati ogni 15 minuti), ne valida l'integrità e li invia a un cluster di messaggistica.
2. **Streaming Layer (Apache Kafka)**: Agisce da broker di messaggi centralizzato, garantendo il buffering e il disaccoppiamento tra la fase di acquisizione dati e quella di elaborazione.
3. **Processing Layer (Apache Spark / PySpark)**: Consuma i flussi continui da Kafka, applica le logiche di parsing, pulizia, arricchimento e calcola gli indici di rischio geospaziali aggregati.
4. **Storage Layer**: Archiviazione dei dati storici e aggregati (es. HDFS / Cassandra / PostgreSQL / Parquet) per consentire analisi storiche e la persistenza dello stato.
5. **Presentation Layer (Dashboard)**: Interfaccia grafica (es. Dash / Streamlit) per la visualizzazione in tempo reale del livello di rischio globale su mappe interattive e grafici temporali.

---

## 📂 Struttura della Repository

```text
risk_bdt/
├── .venv/                  # Ambiente virtuale Python locale
├── data/                   # Cartella locale per lo staging dei dati (Raw/Processed)
│   ├── raw/                # Dati grezzi (CSV e ZIP originari)
│   └── mock_kafka/         # File di test per simulazione pipeline locale
├── dashboard/              # Codice sorgente del Presentation Layer (Frontend)
├── src/                    # Core del progetto (Backend e Processing)
│   ├── ingestion/          # Script di recupero dati e Kafka Producer
│   ├── parsing/            # Logiche di pulizia e mappatura schemi
│   ├── processing/         # Job Spark Streaming per il calcolo delle metriche
│   ├── state/              # File JSON per la persistenza dello stato dell'ingestion
│   ├── storage/            # Connettori e script di configurazione del database
│   └── validation/         # Funzioni per il controllo qualità dei dati
├── docker-compose.yml      # Configurazione dell'infrastruttura (Kafka, Zookeeper, DB)
└── requirements.txt        # Dipendenze Python del progetto

### 2. Configura l'Ambiente Virtuale Python

> ⚠️ **IMPORTANTE (Versione Python):** Il progetto è stato sviluppato e testato utilizzando **Python 3.11** (consigliata versione 3.11.x). Versioni più recenti (es. 3.12+) o più vecchie potrebbero creare conflitti con le librerie di streaming e con i pacchetti di processing.

Crea un ambiente pulito usando esplicitamente Python 3.11 e installa le dipendenze richieste:
```bash
# Forza l'uso di Python 3.11 nella creazione del venv
python3.11 -m venv .venv

# Attiva l'ambiente su macOS/Linux
source .venv/bin/activate

# Aggiorna pip e installa i pacchetti
pip install --upgrade pip
pip install -r requirements.txt
