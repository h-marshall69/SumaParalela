import mysql.connector
from mysql.connector import Error
from multiprocessing import Pool, cpu_count
from functools import partial
import time
import logging
from typing import List, Tuple, Optional
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "test"),
}

def get_connection():
    """Create and return a database connection."""
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Error as e:
        logger.error(f"Error connecting to MySQL database: {e}")
        return None

def get_numbers(start: int, end: int) -> List[int]:
    """Fetch a range of numbers from the database."""
    with get_connection() as conn:
        if conn is None:
            return []
        
        cursor = conn.cursor(buffered=True)
        try:
            query = "SELECT numero FROM suma_paralela WHERE id BETWEEN %s AND %s"
            cursor.execute(query, (start, end))
            return [int(row[0]) for row in cursor]
        except Error as e:
            logger.error(f"Error fetching numbers: {e}")
            return []
        finally:
            cursor.close()

def sum_subset(start_end: Tuple[int, int]) -> int:
    """Sum a subset of numbers."""
    start, end = start_end
    numbers = get_numbers(start, end)
    subset_sum = sum(numbers)
    logger.info(f'Process {os.getpid()} summed {start}-{end}: {subset_sum}')
    return subset_sum

def parallel_sum(total_numbers: int, chunk_size: Optional[int] = None) -> int:
    """Perform parallel summation of numbers."""
    if chunk_size is None:
        chunk_size = max(1, total_numbers // (cpu_count() * 4))  # Adjust based on CPU cores
    
    ranges = [(i, min(i + chunk_size - 1, total_numbers)) for i in range(1, total_numbers + 1, chunk_size)]
    
    with Pool() as pool:
        results = pool.map(sum_subset, ranges)
    
    return sum(results)

def main():
    total_numbers = 1_000_000
    chunk_size = 100_000  # Adjust this based on your specific database and system

    start_time = time.time()
    total_sum = parallel_sum(total_numbers, chunk_size)
    end_time = time.time()

    logger.info(f'Total sum: {total_sum}')
    logger.info(f'Time taken: {end_time - start_time:.2f} seconds')

if __name__ == '__main__':
    main()