from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import json
import os
from pathlib import Path
import pandas as pd

app = FastAPI()

PREFS_DIR = "/data/user_preferences"
PROCESSED_DIR = "/data/processed"
VALIDATED_DATA_PATH = "/data/validated_data.csv"

os.makedirs(PREFS_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

def load_user_prefs(user_id: str) -> dict:
    """Load user preferences from JSON file."""
    prefs_file = Path(PREFS_DIR) / f"{user_id}_prefs.json"
    if not prefs_file.exists():
        raise FileNotFoundError(f"Preferences not found for user {user_id}")
    
    with open(prefs_file, 'r') as f:
        return json.load(f)

def process_data_for_user(user_id: str, prefs: dict) -> dict:
    """
    Process validated data according to user preferences.
    
    Example: If user prefers filtered data, apply filters here.
    """
    if not os.path.exists(VALIDATED_DATA_PATH):
        raise FileNotFoundError("Validated data not found. Run validation layer first.")
    
    df = pd.read_csv(VALIDATED_DATA_PATH)
    
    # Example processing logic based on preferences
    # Customize this based on your actual needs
    processed_df = df.copy()
    
    if prefs.get("data_format") == "summary":
        # Aggregate/summarize data
        processed_df = processed_df.groupby(processed_df.columns[0], as_index=False).agg('mean')
    
    if prefs.get("notifications"):
        # Could add flags or metadata
        processed_df['processed_for'] = user_id
    
    return processed_df

@app.post("/process/{user_id}")
async def process_user(user_id: str):
    """
    On-demand processing endpoint: reads user preferences and generates user-specific dataset.
    """
    try:
        # Load user preferences
        prefs = load_user_prefs(user_id)
        
        # Process data according to preferences
        processed_df = process_data_for_user(user_id, prefs)
        
        # Save user-specific output
        output_file = Path(PROCESSED_DIR) / f"processed_{user_id}.csv"
        processed_df.to_csv(output_file, index=False)
        
        return JSONResponse({
            "status": "success",
            "user_id": user_id,
            "message": f"Data processed for user {user_id}",
            "output_file": str(output_file),
            "rows": len(processed_df)
        })
    
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return JSONResponse({"status": "processing layer is running"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
