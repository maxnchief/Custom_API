# Database Design - Seinfeld Quotes API

This document describes the database schema used for the Seinfeld Quotes API. The database is implemented in PostgreSQL.

---

## Database: `seinfeld_db`

### Table: `seinfeld_quotes`

| Column      | Type                   | Nullable | Description                                        |
|------------|------------------------|----------|--------------------------------------------------|
| `id`       | `SERIAL PRIMARY KEY`   | No       | Unique identifier for each quote                 |
| `quote`    | `TEXT`                 | No       | Text content of the quote                        |
| `author`   | `VARCHAR(255)`         | No       | Name of the character/author                     |
| `season`   | `INT`                  | Yes      | Season number of the show for the quote          |
| `episode`  | `INT`                  | Yes      | Episode number of the show for the quote         |
| `created_at` | `TIMESTAMP`          | Yes      | Timestamp when the record was inserted (default CURRENT_TIMESTAMP) |

---
# Database Design

Here is the Entity-Relationship Diagram for the Seinfeld Quotes database:

![ERD Diagram](images/ERD_Diagram.png)
