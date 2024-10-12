import mysql.connector
from mysql.connector import Error
from mysql.connector.pooling import MySQLConnectionPool
import time
import logging
from typing import List, Tuple
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "test"),
    "pool_name": "mypool",
    "pool_size": 32,
}

# Create a connection pool
try:
    cnx_pool = MySQLConnectionPool(**DB_CONFIG)
    logger.info("Connection pool created successfully.")
except mysql.connector.Error as err:
    logger.error(f"Error creating connection pool: {err}")
    raise

def execute_query(query: str, params: Tuple = None) -> None:
    """Execute a single query."""
    with cnx_pool.get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
        connection.commit()

def setup_database() -> None:
    """Set up the database by dropping and recreating the table."""
    queries = [
        "DROP TABLE IF EXISTS suma_paralela",
        """
        CREATE TABLE suma_paralela (
            id INT AUTO_INCREMENT PRIMARY KEY,
            numero INT NOT NULL
        )
        """,
        "CREATE INDEX idx_numero ON suma_paralela (numero)"
    ]
    for query in queries:
        execute_query(query)
    logger.info("Database setup completed.")

def insert_batch(start: int, end: int) -> int:
    """Insert a batch of numbers into the database."""
    with cnx_pool.get_connection() as connection:
        with connection.cursor() as cursor:
            numeros = [(j,) for j in range(start, end)]
            cursor.executemany("INSERT INTO suma_paralela (numero) VALUES (%s)", numeros)
            connection.commit()
            return cursor.rowcount

def populate_database(total_numeros: int, batch_size: int, max_workers: int) -> None:
    """Populate the database using multiple threads."""
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(1, total_numeros + 1, batch_size):
            end = min(i + batch_size, total_numeros + 1)
            futures.append(executor.submit(insert_batch, i, end))
        
        total_inserted = 0
        for future in as_completed(futures):
            total_inserted += future.result()
            logger.info(f"Batch completed. Total records inserted: {total_inserted}")

    end_time = time.time()
    logger.info(f"Database population completed. Total time: {end_time - start_time:.2f} seconds")

def main():
    total_numeros = 1_000_000
    batch_size = 50_000
    max_workers = 16  # Adjust based on your system's capabilities

    try:
        setup_database()
        populate_database(total_numeros, batch_size, max_workers)
    except Error as ex:
        logger.error(f"An error occurred: {ex}")

if __name__ == "__main__":
    main()