from fastapi import FastAPI, HTTPException
from db import get_db_session
from processing import process
from analysis import get_analysis_results
import requests

app = FastAPI()

@app.post("/process-data")
async def process_data():
    try:
        process()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get-analysis-results")
async def get_analysis_results():
    try:
        results = get_analysis_results()
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
