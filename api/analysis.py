from sqlalchemy.orm import Session
import pandas as pd

def get_analysis_results():
    query = "SELECT * FROM delayed_flights"
    df = pd.read_sql(query, con=engine)
    return df.to_dict(orient="records")
