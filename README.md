# BDT Data Pipeline

A multi-layer data pipeline with ingestion, parsing, validation, processing, and serving layers.

## Getting Started

### Prerequisites
- Docker and Docker Compose installed

### Running the Pipeline

```bash
docker compose up
```

This runs all five layers sequentially:
1. **Ingestion**: Fetch raw data from source
2. **Parsing**: Structure and parse data
3. **Validation**: Validate data quality
4. **Processing**: Transform and compute
5. **Serving**: Expose results via dashboard (http://localhost:8000)

### Project Structure

```
Global-News-Event-Radar-for-Geopolitical-and-Supply-Risk/
├── docker-compose.yml
├── ingestion/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── parsing/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── validation/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── processing/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
└── serving/
    ├── Dockerfile
    ├── requirements.txt
    └── main.py
```

## Contributing

Each layer should handle a specific responsibility. Add dependencies to each layer's `requirements.txt` as needed.

## Licence

TBD
