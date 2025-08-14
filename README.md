# Custom_API
## 🛠 How to Set Up the Local Database

1. Install PostgreSQL
2. Create the database:
    ```bash
    createdb seinfeld
    ```
3. Run the SQL script:
    ```bash
    psql -U your_username -d seinfeld -f setup_db.sql
    ```
4. Load the CSV data:
    ```bash
    python import_csv.py
    ```
