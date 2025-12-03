import os
import logging
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("query-api")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")

client = MongoClient(MONGO_URI)
db = client["psychology_db"]
collection = db["sessions"]

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ready"}

@app.get("/list")
def list_sessions():
    cursor = collection.find({}, {"video_id": 1, "timestamp": 1, "_id": 0})
    return list(cursor)

@app.get("/analysis/{video_id}")
def get_analysis(video_id: str):
    logger.info(f"Fetching analysis for {video_id}")
    
    document = collection.find_one({"video_id": video_id})
    
    if not document:
        raise HTTPException(status_code=404, detail="Analysis not found")
    
    # remove the internal mongo id
    if "_id" in document:
        del document["_id"]
        
    return document

@app.get("/analysis/{video_id}/emotional-arc")
def get_emotional_arc(video_id: str):
    document = collection.find_one(
        {"video_id": video_id}, 
        {"emotional_profile": 1, "_id": 0} 
    )
    
    if not document or "emotional_profile" not in document:
        raise HTTPException(status_code=404, detail="Emotional data not found (Try re-analyzing the video)")
        
    return document["emotional_profile"]

@app.get("/analysis/{video_id}/interventions")
def get_interventions(video_id: str):
    document = collection.find_one(
        {"video_id": video_id}, 
        {"key_interventions": 1, "_id": 0}
    )
    
    if not document or "key_interventions" not in document:
        raise HTTPException(status_code=404, detail="Intervention data not found")
        
    return document["key_interventions"]