#!/usr/bin/env python3
"""
Script to convert Seinfeld.csv data into a MySQL table while preserving formatting.
This script reads the CSV file and creates a MySQL table with appropriate data types.
"""

import csv
import mysql.connector
from mysql.connector import Error
import os
import sys
from typing import List, Tuple, Optional

class SeinfeldCSVToMySQL:
    def __init__(self, csv_file_path: str, mysql_config: dict):
        """
        Initialize the converter with CSV file path and MySQL configuration.
        
        Args:
            csv_file_path: Path to the Seinfeld.csv file
            mysql_config: Dictionary containing MySQL connection parameters
        """
        self.csv_file_path = csv_file_path
        self.mysql_config = mysql_config
        self.connection = None
        self.cursor = None
        
    def connect_to_mysql(self) -> bool:
        """
        Establish connection to MySQL database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = mysql.connector.connect(**self.mysql_config)
            self.cursor = self.connection.cursor()
            print(f"✓ Successfully connected to MySQL database: {self.mysql_config['database']}")
            return True
        except Error as e:
            print(f"✗ Error connecting to MySQL: {e}")
            return False
    
    def create_table(self, table_name: str = "seinfeld_quotes") -> bool:
        """
        Create the seinfeld_quotes table with appropriate schema.
        
        Args:
            table_name: Name of the table to create
            
        Returns:
            bool: True if table created successfully, False otherwise
        """
        try:
            # Drop table if it exists
            drop_query = f"DROP TABLE IF EXISTS {table_name}"
            self.cursor.execute(drop_query)
            
            # Create table with appropriate data types
            create_query = f"""
            CREATE TABLE {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                quote TEXT NOT NULL,
                author VARCHAR(100) NOT NULL,
                season INT NOT NULL,
                episode INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_author (author),
                INDEX idx_season_episode (season, episode)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            self.cursor.execute(create_query)
            self.connection.commit()
            print(f"✓ Table '{table_name}' created successfully")
            return True
            
        except Error as e:
            print(f"✗ Error creating table: {e}")
            return False
    
    def read_csv_data(self) -> List[Tuple[str, str, int, int]]:
        """
        Read and parse the CSV file, preserving formatting.
        
        Returns:
            List of tuples containing (quote, author, season, episode)
        """
        data = []
        
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as csvfile:
                # Use csv.reader to properly handle quoted fields and commas within quotes
                csv_reader = csv.reader(csvfile)
                
                # Skip header row
                next(csv_reader)
                
                for row_num, row in enumerate(csv_reader, start=2):
                    if len(row) >= 4:
                        quote = row[0].strip()
                        author = row[1].strip()
                        
                        try:
                            season = int(row[2].strip())
                            episode = int(row[3].strip())
                        except ValueError as e:
                            print(f"⚠ Warning: Invalid season/episode data in row {row_num}: {e}")
                            continue
                        
                        # Preserve the original formatting of quotes
                        # Remove only outer quotes if they exist (CSV format)
                        if quote.startswith('"') and quote.endswith('"'):
                            quote = quote[1:-1]
                        
                        # Handle escaped quotes within the text
                        quote = quote.replace('""', '"')
                        
                        data.append((quote, author, season, episode))
                    else:
                        print(f"⚠ Warning: Skipping row {row_num} - insufficient columns")
                        
            print(f"✓ Successfully read {len(data)} records from CSV file")
            return data
            
        except FileNotFoundError:
            print(f"✗ Error: CSV file not found: {self.csv_file_path}")
            return []
        except Exception as e:
            print(f"✗ Error reading CSV file: {e}")
            return []
    
    def insert_data(self, data: List[Tuple[str, str, int, int]], table_name: str = "seinfeld_quotes") -> bool:
        """
        Insert data into the MySQL table.
        
        Args:
            data: List of tuples containing quote data
            table_name: Name of the table to insert into
            
        Returns:
            bool: True if insertion successful, False otherwise
        """
        if not data:
            print("✗ No data to insert")
            return False
            
        try:
            insert_query = f"""
            INSERT INTO {table_name} (quote, author, season, episode)
            VALUES (%s, %s, %s, %s)
            """
            
            # Insert data in batches for better performance
            batch_size = 100
            total_inserted = 0
            
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                self.cursor.executemany(insert_query, batch)
                self.connection.commit()
                total_inserted += len(batch)
                print(f"✓ Inserted batch: {total_inserted}/{len(data)} records")
            
            print(f"✓ Successfully inserted all {total_inserted} records into '{table_name}' table")
            return True
            
        except Error as e:
            print(f"✗ Error inserting data: {e}")
            self.connection.rollback()
            return False
    
    def verify_data(self, table_name: str = "seinfeld_quotes") -> bool:
        """
        Verify the inserted data by running some sample queries.
        
        Args:
            table_name: Name of the table to verify
            
        Returns:
            bool: True if verification successful, False otherwise
        """
        try:
            # Count total records
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = self.cursor.fetchone()[0]
            print(f"✓ Total records in table: {total_count}")
            
            # Show sample records
            self.cursor.execute(f"""
                SELECT id, LEFT(quote, 50) as quote_preview, author, season, episode 
                FROM {table_name} 
                ORDER BY season, episode 
                LIMIT 5
            """)
            
            print("\n📋 Sample records:")
            print("ID | Quote Preview | Author | Season | Episode")
            print("-" * 70)
            
            for row in self.cursor.fetchall():
                quote_preview = row[1][:47] + "..." if len(row[1]) > 50 else row[1]
                print(f"{row[0]:2d} | {quote_preview:50s} | {row[2]:8s} | {row[3]:6d} | {row[4]:7d}")
            
            # Show statistics by character
            self.cursor.execute(f"""
                SELECT author, COUNT(*) as quote_count 
                FROM {table_name} 
                GROUP BY author 
                ORDER BY quote_count DESC
            """)
            
            print(f"\n📊 Quotes by character:")
            print("Character | Quote Count")
            print("-" * 25)
            
            for row in self.cursor.fetchall():
                print(f"{row[0]:15s} | {row[1]:5d}")
            
            return True
            
        except Error as e:
            print(f"✗ Error during verification: {e}")
            return False
    
    def close_connection(self):
        """Close the MySQL connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("✓ MySQL connection closed")
    
    def convert(self, table_name: str = "seinfeld_quotes") -> bool:
        """
        Main method to perform the complete conversion process.
        
        Args:
            table_name: Name of the table to create and populate
            
        Returns:
            bool: True if conversion successful, False otherwise
        """
        print("🎬 Starting Seinfeld CSV to MySQL conversion...")
        print("=" * 60)
        
        # Connect to MySQL
        if not self.connect_to_mysql():
            return False
        
        try:
            # Create table
            if not self.create_table(table_name):
                return False
            
            # Read CSV data
            data = self.read_csv_data()
            if not data:
                return False
            
            # Insert data
            if not self.insert_data(data, table_name):
                return False
            
            # Verify data
            if not self.verify_data(table_name):
                return False
            
            print("\n🎉 Conversion completed successfully!")
            return True
            
        finally:
            self.close_connection()


def main():
    """Main function to run the conversion."""
    
    # Configuration
    csv_file_path = "/home/maxnchief/Custom_API/Seinfeld.csv"
    
    # MySQL connection configuration
    # You'll need to update these values for your MySQL setup
    mysql_config = {
        'host': 'localhost',        # MySQL server host
        'port': 3306,               # MySQL server port
        'user': 'your_username',    # MySQL username
        'password': 'your_password', # MySQL password
        'database': 'seinfeld_db',  # Database name
        'charset': 'utf8mb4',
        'collation': 'utf8mb4_unicode_ci'
    }
    
    # Check if CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"✗ Error: CSV file not found at {csv_file_path}")
        sys.exit(1)
    
    # Get MySQL configuration from user
    print("🔧 MySQL Configuration")
    print("=" * 30)
    
    # You can uncomment these lines to prompt for MySQL credentials
    # mysql_config['host'] = input(f"MySQL Host [{mysql_config['host']}]: ") or mysql_config['host']
    # mysql_config['port'] = int(input(f"MySQL Port [{mysql_config['port']}]: ") or mysql_config['port'])
    # mysql_config['user'] = input(f"MySQL Username [{mysql_config['user']}]: ") or mysql_config['user']
    # mysql_config['password'] = input("MySQL Password: ")
    # mysql_config['database'] = input(f"Database Name [{mysql_config['database']}]: ") or mysql_config['database']
    
    print(f"Host: {mysql_config['host']}:{mysql_config['port']}")
    print(f"Database: {mysql_config['database']}")
    print(f"User: {mysql_config['user']}")
    print()
    
    # Create converter instance and run conversion
    converter = SeinfeldCSVToMySQL(csv_file_path, mysql_config)
    
    table_name = input("Table name [seinfeld_quotes]: ") or "seinfeld_quotes"
    
    success = converter.convert(table_name)
    
    if success:
        print(f"\n✅ All done! Your Seinfeld quotes are now in the '{table_name}' table.")
        print("\nSample queries you can run:")
        print(f"  SELECT * FROM {table_name} WHERE author = 'Jerry' LIMIT 10;")
        print(f"  SELECT author, COUNT(*) FROM {table_name} GROUP BY author;")
        print(f"  SELECT * FROM {table_name} WHERE season = 1 ORDER BY episode;")
    else:
        print("\n❌ Conversion failed. Please check the error messages above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
