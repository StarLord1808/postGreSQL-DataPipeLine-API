import psycopg2
from sqlalchemy import create_engine

# Database configuration
DB_CONFIG = {
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432',
    'dbname': 'hospital',
    'schema': 'LANDING_LAYER'
}

# psycopg2 Connection Test
try:
    conn = psycopg2.connect(
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port']
    )
    print("psycopg2 connected to PostgreSQL from WSL!")
    conn.close()
except Exception as e:
    print(f"psycopg2 connection failed: {e}")

# SQLAlchemy Engine Test
try:
    connection_string = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        print("SQLAlchemy connected to PostgreSQL from WSL!")
except Exception as e:
    print(f"SQLAlchemy connection failed: {e}")