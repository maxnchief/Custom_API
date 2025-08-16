import csv
import logging
import os
import sys
from typing import List, Tuple
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),           # console
        logging.FileHandler("seinfeld_loader.log")   # file
    ]
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV_PATH = os.path.join(SCRIPT_DIR, "Seinfeld.csv")

# PostgreSQL config from environment
PG_CONFIG = {
    'host': os.getenv("PG_HOST"),
    'port': int(os.getenv("PG_PORT", 5432)),
    'user': os.getenv("PG_USER"),
    'password': os.getenv("PG_PASSWORD"),
    'dbname': os.getenv("PG_DBNAME")
}

class SeinfeldLoader:
    def __init__(self, csv_file_path: str, pg_config: dict = PG_CONFIG):
        self.csv_file_path = csv_file_path
        self.pg_config = pg_config
        self.connection = None
        self.cursor = None

    def connect_to_postgres(self) -> bool:
        """Connect to an existing PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(**self.pg_config)
            self.cursor = self.connection.cursor()
            logging.info(f"Connected to PostgreSQL database: {self.pg_config['dbname']}")
            return True
        except Exception as e:
            logging.error(f"Error connecting to PostgreSQL: {e}")
            return False

    def create_table(self, table_name: str = "seinfeld_quotes") -> bool:
        """Create the table if it does not exist."""
        try:
            create_query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    quote TEXT NOT NULL,
                    author VARCHAR(100) NOT NULL,
                    season INT NOT NULL,
                    episode INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format(sql.Identifier(table_name))
            self.cursor.execute(create_query)
            self.connection.commit()
            logging.info(f"Table '{table_name}' is ready")
            return True
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            return False

    def read_csv_data(self) -> List[Tuple[str, str, int, int]]:
        """Read and parse CSV file into a list of tuples."""
        data = []
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)  # Skip header
                for row_num, row in enumerate(reader, start=2):
                    if len(row) >= 4:
                        try:
                            data.append((row[0].strip(), row[1].strip(), int(row[2]), int(row[3])))
                        except ValueError as ve:
                            logging.warning(f"Invalid season/episode in row {row_num}: {ve}")
                    else:
                        logging.warning(f"Skipping row {row_num} - insufficient columns")
            logging.info(f"Read {len(data)} records from CSV")
            return data
        except FileNotFoundError:
            logging.error(f"CSV file not found: {self.csv_file_path}")
            return []

    def insert_data(self, data: List[Tuple[str, str, int, int]], table_name: str = "seinfeld_quotes") -> bool:
        """Insert rows into PostgreSQL."""
        if not data:
            logging.error("No data to insert")
            return False
        try:
            insert_query = sql.SQL("""
                INSERT INTO {} (quote, author, season, episode)
                VALUES (%s, %s, %s, %s)
            """).format(sql.Identifier(table_name))
            self.cursor.executemany(insert_query, data)
            self.connection.commit()
            logging.info(f"Inserted {len(data)} records into '{table_name}'")
            return True
        except Exception as e:
            logging.error(f"Error inserting data: {e}")
            self.connection.rollback()
            return False

    def close_connection(self):
        """Close PostgreSQL connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logging.info("PostgreSQL connection closed")

    def convert(self, table_name: str = "seinfeld_quotes") -> bool:
        """Run the CSV → PostgreSQL load process."""
        logging.info("Starting Seinfeld CSV to PostgreSQL conversion")
        if not self.connect_to_postgres():
            return False
        try:
            if not self.create_table(table_name):
                return False
            data = self.read_csv_data()
            if not self.insert_data(data, table_name):
                return False
            logging.info("Conversion completed successfully")
            return True
        finally:
            self.close_connection()
