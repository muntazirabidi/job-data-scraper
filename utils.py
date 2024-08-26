import sqlite3
from contextlib import contextmanager
import logging
import time

# Context manager for database connection
@contextmanager
def db_connection(db_name='job_listings.db'):
    conn = sqlite3.connect(db_name)
    try:
        yield conn
    finally:
        conn.close()

def setup_database(conn):
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        category TEXT,
        title TEXT,
        company TEXT,
        location TEXT,
        salary TEXT,
        url TEXT UNIQUE,
        description TEXT,
        detailed_salary TEXT,
        job_type TEXT,
        detailed_location TEXT,
        date_posted TEXT,
        date_scraped DATE)''')
    conn.commit()
    logging.info("Database setup complete")

def insert_jobs_batch(conn, jobs):
    c = conn.cursor()
    try:
        c.executemany('''INSERT OR REPLACE INTO jobs
            (source, category, title, company, location, salary, url, description, detailed_salary, job_type, detailed_location, date_posted, date_scraped)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            jobs)
        conn.commit()
        logging.info(f"Successfully inserted {len(jobs)} jobs into the database")
    except sqlite3.Error as e:
        logging.error(f"An error occurred while inserting jobs: {e}")

def retry_operation(operation, retries=3, delay=2):
    for attempt in range(retries):
        try:
            return operation()
        except Exception as e:
            logging.warning(f"Operation failed on attempt {attempt+1}/{retries}: {e}")
            time.sleep(delay)
    raise Exception("Operation failed after maximum retries")