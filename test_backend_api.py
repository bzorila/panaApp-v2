from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import json

app = FastAPI()

# Database configuration
DB_CONFIG = {
    "dbname": "panasonic",
    "user": "pana_app",
    "password": "p4na@p9$",
    "host": "172.17.1.100",
    "port": "5432"
}

# Class for parameters table
class ParameterData(BaseModel):
    timeStamp: str
    parameterSet: str
    validatedAt: str
    validBy: int
    readerType: int  # New field

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

@app.get("/")
async def home():
    return {"message" : "Hello into test mode!"}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("__main__:app", host="0.0.0.0", port=8000, reload=True)