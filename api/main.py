from fastapi import FastAPI, HTTPException
from db import get_db_session
from processing import process
import analysis
import requests

app = FastAPI()

@app.post("/process-data")
async def process_data():
    try:
        process()
        return {
            "success": True,
            "message": "Data processing complete."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get-analysis-results")
async def get_analysis_results():
    try:
        results = analysis.get_analysis_results()
        return {
            "success": True,
            "results": results
        }
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
