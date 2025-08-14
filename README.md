# Custom_API
## 🛠 How to Set Up the Local Database

1. Install PostgreSQL
2. Create the database:
    ```bash
    createdb seinfeld
    ```
3. Run the SQL script:
    ```bash
    psql -U your_username -d seinfeld -f schema.sql
    ```
4. run this in the psql terminal
    \copy seinfeld_quotes(quote, character, season, episode) FROM 'Seinfeld.csv' CSV HEADER;

    ```
