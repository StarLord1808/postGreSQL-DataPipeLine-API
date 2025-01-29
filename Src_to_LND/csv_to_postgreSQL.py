import pandas as pd
from psycopg2.extras import execute_values
import psycopg2
import logging
from datetime import datetime
import shutil
import os
import glob

# Generate log file name with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_path = f'/home/starlord/ETL_PostgreSQL/Subhasis_Tasks/postGreSQL-DataPipeLine-API/Src_to_LND/logs/csv_to_postgresql_{timestamp}.log'

# Configure logging
logging.basicConfig(filename=log_file_path, level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Paths
CSV_DIR = r'/home/starlord/ETL_PostgreSQL/Subhasis_Tasks/postGreSQL-DataPipeLine-API/Src_to_LND/Source_Path/'  # Directory containing CSV files
ARCHIVE_DIR = r'/home/starlord/ETL_PostgreSQL/Subhasis_Tasks/postGreSQL-DataPipeLine-API/Src_to_LND/Archive_Path'  # Directory for processed files
ERROR_DIR = r'/home/starlord/ETL_PostgreSQL/Subhasis_Tasks/postGreSQL-DataPipeLine-API/Src_to_LND/Bad_Files/'  # Directory for error files

# Table and database configuration
TABLE_NAME = "appointments"  # Table name without schema
DB_CONFIG = {
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432',
    'dbname': 'hospital',
    'schema': 'LANDING_LAYER'  # Add schema to the configuration
}

def truncate_table(table_name, db_config):
    """
    Truncate the target table before processing CSV files.
    """
    try:
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        cursor = conn.cursor()

        schema_quoted = f'"{db_config["schema"]}"'
        table_quoted = f'"{table_name}"'
        logging.info(f"Truncating table: {schema_quoted}.{table_quoted}")
        cursor.execute(f"TRUNCATE TABLE {schema_quoted}.{table_quoted} RESTART IDENTITY;")
        conn.commit()
        logging.info(f"Table {db_config['schema']}.{table_name} truncated successfully.")
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error truncating table {db_config['schema']}.{table_name}: {e}")
        raise

def insert_data_with_psycopg2(df, table_name, db_config):
    """
    Insert data into PostgreSQL using psycopg2.extras.execute_values for bulk insertion.
    """
    try:
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        cursor = conn.cursor()

        # Build the SQL INSERT statement
        columns = ', '.join(df.columns)
        values_placeholder = ', '.join(['%s'] * len(df.columns))
        schema_quoted = f'"{db_config["schema"]}"'
        insert_query = f"""
            INSERT INTO {schema_quoted}.{table_name} ({columns})
            VALUES %s
        """

        # Convert DataFrame to a list of tuples for psycopg2
        data = df.values.tolist()

        # Use execute_values for bulk insertion
        execute_values(cursor, insert_query, data)
        conn.commit()

        logging.info(f"Data successfully inserted into {db_config['schema']}.{table_name}.")
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        raise

def load_csv_to_postgres_with_archiving(csv_file_path, table_name, db_config, archive_dir, error_dir):
    try:
        # Load CSV into Pandas DataFrame
        df = pd.read_csv(csv_file_path)
        logging.info(f"Data loaded from {csv_file_path}.")
        
        # Convert column names to lowercase
        df.columns = map(str.lower, df.columns)
        
        # Separate valid and invalid rows
        valid_rows = []
        invalid_rows = []

        # Validate data row by row
        for index, row in df.iterrows():
            try:
                # Example validation: Check for missing or invalid data
                if row.isnull().any() or (row['age'] < 0):  # Invalid if age is negative or any column is missing
                    raise ValueError(f"Invalid data: {row.to_dict()}")
                valid_rows.append(row)
            except Exception as e:
                invalid_rows.append(row)
                logging.error(f"Invalid row at index {index}: {row.to_dict()}, Error: {e}")

        # Convert valid rows back to a DataFrame
        valid_df = pd.DataFrame(valid_rows)
        
        # Insert valid rows into PostgreSQL
        if not valid_df.empty:
            insert_data_with_psycopg2(valid_df, table_name, db_config)

        # Write invalid rows to the error file
        if invalid_rows:
            invalid_df = pd.DataFrame(invalid_rows)
            error_file_path = os.path.join(error_dir, f'invalid_records_{timestamp}.csv')
            invalid_df.to_csv(error_file_path, index=False)
            logging.warning(f"Invalid rows written to {error_file_path}.")

        # Move the processed CSV file to the archive directory
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)  # Create archive directory if it doesn't exist
        
        archive_path = os.path.join(archive_dir, os.path.basename(csv_file_path))
        shutil.move(csv_file_path, archive_path)
        logging.info(f"Source file {csv_file_path} moved to archive directory: {archive_path}.")
    except Exception as e:
        logging.error(f"Error: {e}")

def process_all_csv_files(csv_dir, table_name, db_config, archive_dir, error_dir):
    """
    Process all CSV files in the specified directory.
    """
    # Get a list of all CSV files in the directory
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))

    if not csv_files:
        logging.info("No CSV files found to process.")
        print("No CSV files found to process.")
        return

    # Truncate the table before processing files
    truncate_table(table_name, db_config)

    for csv_file in csv_files:
        print(f"Processing file: {csv_file}")
        logging.info(f"Processing file: {csv_file}")
        load_csv_to_postgres_with_archiving(csv_file, table_name, db_config, archive_dir, error_dir)

# Main function to process all files
if __name__ == "__main__":
    process_all_csv_files(CSV_DIR, TABLE_NAME, DB_CONFIG, ARCHIVE_DIR, ERROR_DIR)
