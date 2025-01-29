# FastAPI-Based PostgreSQL Data Retrieval API

## Overview
This FastAPI application provides an API to retrieve data from a PostgreSQL database based on various filters such as schema, table name, timestamp, and ID. It allows:
- Fetching all tables within a given schema.
- Retrieving all records from a specific table.
- Filtering records based on timestamp range or specific ID.
- Logging API requests, SQL queries, and errors for debugging.

---

## Repository
GitHub Repository: [postGreSQL-DataPipeLine-API](https://github.com/StarLord1808/postGreSQL-DataPipeLine-API.git)

---

## Setup Guide

### 1. WSL (Windows Subsystem for Linux) Setup
#### Install WSL and Ubuntu
```sh
wsl --install -d Ubuntu
```
#### Set Up PostgreSQL in WSL
Follow the PostgreSQL installation steps from below.

---

### 2. Python Setup
#### Install Python & Virtual Environment
```sh
sudo apt install python3 python3-pip python3-venv
python3 -m venv venv
source venv/bin/activate
```
#### Install Required Dependencies
```sh
pip install -r requirements.txt
```

---

### 3. PostgreSQL Setup
#### Install PostgreSQL on Ubuntu (WSL/Linux)
```sh
sudo apt update
sudo apt install postgresql postgresql-contrib
```
#### Start and Enable PostgreSQL
```sh
sudo systemctl start postgresql
sudo systemctl enable postgresql
```
#### Create a Database and User
```sh
sudo -i -u postgres
psql
CREATE DATABASE hospital;
CREATE USER postgres WITH ENCRYPTED PASSWORD 'postgres';
ALTER ROLE postgres SUPERUSER;
```
#### Allow Remote Connections (Optional)
Edit `postgresql.conf` and `pg_hba.conf` for external access:
```sh
sudo nano /etc/postgresql/14/main/postgresql.conf
listen_addresses = '*'
```
```sh
sudo nano /etc/postgresql/14/main/pg_hba.conf
host    all             all             0.0.0.0/0               md5
```
Restart PostgreSQL:
```sh
sudo systemctl restart postgresql
```

---

### 4. Airflow Setup
#### Install Apache Airflow
```sh
pip install apache-airflow
```
#### Initialize Airflow Database
```sh
airflow db init
```
#### Start Airflow Scheduler & Webserver
```sh
airflow scheduler &
airflow webserver -p 8080 &
```
#### Access Airflow UI
Open a browser and go to `http://localhost:8080`

---

## Running the API

### Step 1: Start the FastAPI Server
```sh
$ uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

### Step 2: Test API in Swagger UI
Once the server is running, open your browser and go to:
- **Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc Documentation**: [http://localhost:8000/redoc](http://localhost:8000/redoc)

---

## Debugging and Fixes Implemented

### 1. **500 Internal Server Error - Schema Case Sensitivity**
#### Solution:
- Use double quotes around schema and table names:
```python
schema_quoted = f'"{schema}"'
table_quoted = f'"{table}"'
query = f"SELECT * FROM {schema_quoted}.{table_quoted}"
```

---

### 2. **Error: Could Not Import Module 'app' (While Running Uvicorn)**
#### Solution:
- Run the correct command based on the file structure:
```sh
$ uvicorn app:app --reload --host 0.0.0.0 --port 8000
```
- If inside a folder:
```sh
$ uvicorn src.app:app --reload --host 0.0.0.0 --port 8000
```

---

### 3. **Incorrect Query Formatting for Parameterized SQL Queries**
#### Solution:
```python
query = """
    SELECT table_name FROM information_schema.tables WHERE table_schema = %s
"""
cursor.execute(query, (schema,))
```

---

### 4. **Dependency Conflicts (SQLAlchemy & Pandas)**
#### Issue:
- `pip install` caused conflicts due to incompatible versions of `SQLAlchemy` and `pandas`.
#### Solution:
- Downgrade `SQLAlchemy` to a compatible version:
```sh
pip install SQLAlchemy==1.4.36
```

---

### 5. **WSL PostgreSQL Connection Issues**
#### Issue:
- PostgreSQL refused connections when accessed from Windows applications.
#### Solution:
- Ensure PostgreSQL is listening on all interfaces:
```sh
sudo nano /etc/postgresql/14/main/postgresql.conf
listen_addresses = '*'
```
- Restart PostgreSQL:
```sh
sudo systemctl restart postgresql
```
- If using Windows applications to connect, ensure ports are open in Windows Firewall.

---

## Conclusion
This FastAPI-based PostgreSQL API enables flexible data retrieval with built-in logging and debugging capabilities. If you encounter issues, check the log files and ensure that schema and table names are correctly quoted. 

For further improvements, consider adding authentication and pagination.

---

### Authors
- **Your Name**
- **Contributors: Open to Pull Requests!**

