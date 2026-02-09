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

def build_insert_query():
    """Build the full INSERT query with all 256 columns"""
    
    # Start with basic columns
    columns = [
        'read_at',
        'parameter_set', 
        'validated_at',
        'valid_by',
        'reader_type'
    ]
    
    # Add 256 parameter columns (00 to FF)
    for i in range(256):
        hex_val = format(i, '02X').upper()
        columns.append(f'parameter{hex_val}')
    
    # Build the query with parameterized placeholders
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    
    return f"""
        INSERT INTO parameter_data ({columns_str})
        VALUES ({placeholders})
        RETURNING id
    """

@app.post("/api/parameters")
async def receive_parameters(data: ParameterData):
    """Insert parameter data with all 256 columns"""
    try:
        # Parse timestamps
        read_at = datetime.strptime(data.timeStamp, "%Y-%m-%d %H:%M:%S")
        validated_at = datetime.strptime(data.validatedAt, "%Y-%m-%d %H:%M:%S")
        
        # Split parameterSet into 256 chunks (already space-separated)
        chunks = data.parameterSet.split(' ')
        
        # Verify we have exactly 256 chunks
        if len(chunks) != 256:
            raise ValueError(f"Expected 256 chunks, got {len(chunks)}")
        
        # Verify each chunk is 4 characters
        for i, chunk in enumerate(chunks):
            if len(chunk) != 4:
                raise ValueError(f"Chunk {i} ('{chunk}') is not 4 characters")
        
        # Build parameter values list
        values = [
            read_at,
            data.parameterSet,  # Store original string with spaces
            validated_at,
            data.validBy,
            data.readerType
        ]
        
        # Add all 256 chunks
        values.extend(chunks)
        
        # Connect to database and execute
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the pre-built query
        insert_query = build_insert_query()
        
        # Execute the query
        cursor.execute(insert_query, values)
        inserted_id = cursor.fetchone()[0]
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": "Data inserted successfully",
            "inserted_id": inserted_id,
            "chunks_count": len(chunks)
        }
        
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# Alternative endpoint using JSON directly
@app.post("/api/parameters/json")
async def receive_parameters_json(data: dict):
    """
    Receive parameter data as JSON and store it in PostgreSQL
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Convert dict to JSON string for PostgreSQL
        json_data = json.dumps(data)
        
        # Call the JSON version of the procedure
        cursor.callproc("insert_parameter_data_json", [json_data])
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "status": "success", 
            "message": "Data inserted from JSON",
            "reader_type": data.get('readerType', 'not provided')
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# New endpoint for batch insertion
@app.post("/api/parameters/batch")
async def receive_parameters_batch(data_list: list[ParameterData]):
    """
    Receive multiple parameter records in batch
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        results = []
        for data in data_list:
            read_at = datetime.strptime(data.timeStamp, "%Y-%m-%d %H:%M:%S")
            validated_at = datetime.strptime(data.validatedAt, "%Y-%m-%d %H:%M:%S")
            
            cursor.callproc(
                "insert_parameter_data",
                [
                    read_at, 
                    data.parameterSet, 
                    validated_at, 
                    data.validBy,
                    data.readerType
                ]
            )
            
            results.append({
                "timestamp": data.timeStamp,
                "reader_type": data.readerType,
                "status": "inserted"
            })
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": f"Inserted {len(data_list)} records",
            "results": results
        }
        
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("__main__:app", host="0.0.0.0", port=8000, reload=True)