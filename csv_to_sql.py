#!/usr/bin/env python3
import sqlite3
import csv
import pandas as pd

def create_sqlite_database():
    """Convert CSV to SQLite database"""
    
    # Connect to SQLite database (creates if doesn't exist)
    conn = sqlite3.connect('seinfeld_quotes.db')
    cursor = conn.cursor()
    
    # Create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS seinfeld_quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote TEXT NOT NULL,
            author TEXT,
            season INTEGER,
            episode INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Read CSV and insert data
    with open('Seinfeld.csv', 'r', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO seinfeld_quotes (quote, author, season, episode)
                VALUES (?, ?, ?, ?)
            ''', (
                row['quote'],
                row['author'],
                int(row['season']) if row['season'].isdigit() else None,
                int(row['episode']) if row['episode'].isdigit() else None
            ))
    
    # Commit and close
    conn.commit()
    print(f"Inserted {cursor.rowcount} records into SQLite database")
    conn.close()

def create_mysql_script():
    """Generate MySQL import script"""
    
    with open('create_seinfeld_table.sql', 'w') as sqlfile:
        # Write table creation
        sqlfile.write('''-- Create Seinfeld Quotes Database
CREATE DATABASE IF NOT EXISTS seinfeld_db;
USE seinfeld_db;

-- Create table
CREATE TABLE IF NOT EXISTS seinfeld_quotes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    quote TEXT NOT NULL,
    author VARCHAR(100),
    season INT,
    episode INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Import CSV data (adjust path as needed)
LOAD DATA INFILE '/path/to/Seinfeld.csv'
INTO TABLE seinfeld_quotes
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\\n'
IGNORE 1 ROWS
(quote, author, season, episode);

-- Create useful indexes
CREATE INDEX idx_author ON seinfeld_quotes(author);
CREATE INDEX idx_season_episode ON seinfeld_quotes(season, episode);

-- Sample queries
SELECT COUNT(*) as total_quotes FROM seinfeld_quotes;
SELECT author, COUNT(*) as quote_count FROM seinfeld_quotes GROUP BY author ORDER BY quote_count DESC;
''')
    
    print("Created MySQL script: create_seinfeld_table.sql")

def pandas_to_sql():
    """Using pandas for database operations"""
    
    # Read CSV
    df = pd.read_csv('Seinfeld.csv')
    
    # Connect to SQLite
    conn = sqlite3.connect('seinfeld_quotes_pandas.db')
    
    # Write to SQL table
    df.to_sql('seinfeld_quotes', conn, if_exists='replace', index=False)
    
    print("Data imported using pandas")
    conn.close()

if __name__ == "__main__":
    print("Choose conversion method:")
    print("1. SQLite database")
    print("2. MySQL script")
    print("3. Pandas to SQL")
    
    choice = input("Enter choice (1-3): ")
    
    if choice == "1":
        create_sqlite_database()
    elif choice == "2":
        create_mysql_script()
    elif choice == "3":
        pandas_to_sql()
    else:
        print("Invalid choice")
