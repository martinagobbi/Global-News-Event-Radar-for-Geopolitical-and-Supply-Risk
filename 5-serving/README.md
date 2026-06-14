# 5-serving

This serving layer is split into two parts.

## Folders

- `frontend/`: Streamlit dashboard. It owns UI, navigation, forms, maps, event cards, and buttons. It does not read or write the shared `data/` folder.
- `backend/`: FastAPI service. It owns persistent user profiles, preferences, event tags, archive state, user-specific gold layer reads, and pipeline status.
- `data/`: shared Docker volume for backend and processing services. The frontend must not mount this folder.

## Data Ownership

Persistent backend data lives under `data/`:

- `data/users/{user_id}.json`: user preferences.
- `data/tags/{user_id}.json`: user traffic-light tags for events.
- `data/gold/{user_id}.json`: user-specific gold layer and `timestamp_of_last_update`.
- `data/status/pipeline_status.json`: technical pipeline status.

The pipeline status file is flat:

```json
{
  "status": "OK"
}
```

If `status` is `ERROR`, the frontend shows:

```text
Due to technical difficulties, this dashboard has not been updated since {timestamp_of_last_update}.
```

## Run

```bash
docker compose up --build
```

Open:

```text
http://localhost:8501?user=demo_logistics
```

Second demo account:

```text
http://localhost:8501?user=demo_energy
```

## Workflow

1. The user opens the Streamlit frontend.
2. Streamlit asks the backend for the user profile and briefing.
3. When preferences change, Streamlit sends them to the backend.
4. The backend stores preferences in `data/users/` and requests a gold-layer refresh.
5. The processing layer will later read preferences and write updated user gold layers into `data/gold/`.
6. Streamlit keeps reading through the backend API only.