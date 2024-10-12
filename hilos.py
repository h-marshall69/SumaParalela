import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
import logging
from typing import List, Tuple, Optional
import os
from functools import partial
from contextlib import contextmanager

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

@contextmanager
def get_connection():
    """Get a connection from the pool."""
    conn = cnx_pool.get_connection()
    try:
        yield conn
    finally:
        conn.close()

def get_numbers(start: int, end: int) -> List[int]:
    """Fetch a range of numbers from the database."""
    with get_connection() as conn:
        cursor = conn.cursor(buffered=True)
        try:
            query = "SELECT numero FROM suma_paralela WHERE id BETWEEN %s AND %s"
            cursor.execute(query, (start, end))
            return [int(row[0]) for row in cursor]
        except mysql.connector.Error as err:
            logger.error(f"Error fetching numbers: {err}")
            return []
        finally:
            cursor.close()

def sum_subset(start_end: Tuple[int, int]) -> int:
    """Sum a subset of numbers."""
    start, end = start_end
    numbers = get_numbers(start, end)
    subset_sum = sum(numbers)
    logger.info(f'Thread summed {start}-{end}: {subset_sum}')
    return subset_sum

def threaded_sum(total_numbers: int, chunk_size: Optional[int] = None, max_workers: Optional[int] = None) -> int:
    """Perform threaded summation of numbers."""
    if chunk_size is None:
        chunk_size = 100000  # Adjust based on your specific needs
    if max_workers is None:
        max_workers = min(32, (total_numbers + chunk_size - 1) // chunk_size)  # Adjust based on your system

    ranges = [(i, min(i + chunk_size - 1, total_numbers)) for i in range(1, total_numbers + 1, chunk_size)]
    
    total_sum = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_range = {executor.submit(sum_subset, r): r for r in ranges}
        for future in as_completed(future_to_range):
            total_sum += future.result()
    
    return total_sum

def main():
    total_numbers = 1_000_000
    chunk_size = 100_000  # Adjust this based on your specific database and system
    max_workers = 16  # Adjust based on your system's capabilities

    start_time = time.time()
    total_sum = threaded_sum(total_numbers, chunk_size, max_workers)
    end_time = time.time()

    logger.info(f'Total sum: {total_sum}')
    logger.info(f'Time taken: {end_time - start_time:.2f} seconds')

if __name__ == '__main__':
    main()