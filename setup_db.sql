-- setup_db.sql
DROP TABLE IF EXISTS seinfeld_quotes;

CREATE TABLE seinfeld_quotes (
  id SERIAL PRIMARY KEY,
  quote TEXT NOT NULL,
  character VARCHAR(255) NOT NULL,
  season INT,
  episode INT
);
