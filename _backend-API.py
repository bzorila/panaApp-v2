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


    
@app.post("/api/parameters")
async def receive_parameters(data: ParameterData):
    """
    Receive parameter data with readerType and store it in PostgreSQL
    """
    try:
        # Validate timestamps
        read_at = datetime.strptime(data.timeStamp, "%Y-%m-%d %H:%M:%S")
        validated_at = datetime.strptime(data.validatedAt, "%Y-%m-%d %H:%M:%S")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Call the function (not procedure)
        cursor.callproc(
            "function_insert_parameter_data",
            [
                read_at, 
                data.parameterSet, 
                validated_at, 
                data.validBy,
                data.readerType
            ]
        )
        
        # Get the returned ID
        result = cursor.fetchone()
        inserted_id = result[0] if result else None
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": "Data inserted successfully",
            "inserted_id": inserted_id,
            "read_at": data.timeStamp,
            "validated_at": data.validatedAt,
            "validated_by": data.validBy,
            "reader_type": data.readerType
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("__main__:app", host="0.0.0.0", port=8000, reload=True)
