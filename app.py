from fastapi import FastAPI, HTTPException, Query, Request
from typing import Optional
import psycopg2
import logging
from datetime import datetime

# Initialize FastAPI app
app = FastAPI()

# Generate log file name with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_path = f"/home/starlord/ETL_PostgreSQL/Subhasis_Tasks/postGreSQL-DataPipeLine-API/fastapi_retrieve_{timestamp}.log"

# Configure logging
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
)

# Database configuration
DB_CONFIG = {
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
    "dbname": "core",
}


def get_db_connection():
    """
    Establish and return a connection to the PostgreSQL database.
    """
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG["dbname"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        raise HTTPException(status_code=500, detail="Failed to connect to the database.")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """
    Middleware to log incoming requests.
    """
    logging.info(f"Request: {request.method} {request.url}")
    response = await call_next(request)
    logging.info(f"Response status: {response.status_code}")
    return response


@app.get("/retrieve-data")
async def retrieve_data(
    schema: str = Query(..., description="Schema name to query"),
    table: Optional[str] = Query(None, description="Table name to query"),
    timestamp_column: Optional[str] = Query(None, description="Name of the timestamp column for filtering"),
    start_time: Optional[str] = Query(None, description="Start time for filtering (YYYY-MM-DD)"),
    end_time: Optional[str] = Query(None, description="End time for filtering (YYYY-MM-DD)"),
    id_column: Optional[str] = Query(None, description="ID column name for filtering"),
    id_value: Optional[int] = Query(None, description="ID value for filtering"),
):
    """
    Retrieve data from PostgreSQL based on schema, table, timestamp, and ID filters.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Case 1: If only schema is provided, retrieve all tables in the schema
        if not table:
            query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
            """
            logging.info(f"Executing query: {query} with schema={schema}")
            cursor.execute(query, (schema,))
            tables = cursor.fetchall()
            table_list = [table[0] for table in tables]
            cursor.close()
            conn.close()
            logging.info(f"Retrieved tables: {table_list}")
            return {"tables": table_list}

        # Case 2: If schema and table are provided, fetch data from the table
        schema_quoted = f'"{schema}"'
        table_quoted = f'"{table}"'
        query = f"SELECT * FROM {schema_quoted}.{table_quoted}"


        # Add optional filters
        filters = []
        params = []

        if timestamp_column and start_time and end_time:
            filters.append(f"{timestamp_column} BETWEEN %s AND %s")
            params.extend([start_time, end_time])

        if id_column and id_value:
            filters.append(f"{id_column} = %s")
            params.append(id_value)

        # Append filters to the query
        if filters:
            query += " WHERE " + " AND ".join(filters) 

        logging.info(f"Executing query: {query} with params={params}")
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        # Convert the result to JSON format
        data = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        conn.close()

        logging.info(f"Query executed successfully. Rows retrieved: {len(data)}")
        return {"data": data}
    except HTTPException as e:
        raise e
    except Exception as e:
        logging.error(f"Error retrieving data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")
