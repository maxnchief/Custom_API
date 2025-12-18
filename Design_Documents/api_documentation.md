# Seinfeld Quotes API Documentation

This API provides access to quotes from the Seinfeld TV show. You can retrieve all quotes (paginated) or filter quotes by a specific character/author. All responses are in JSON format.

---

## Endpoints Overview

| Endpoint                 | Method | Description                                      | Parameters                                    |
|---------------------------|--------|--------------------------------------------------|-----------------------------------------------|
| `/quotes`                 | GET    | Retrieve all quotes (paginated)                 | `page` (optional, int), `per_page` (optional, int) |
| `/quotes/<author>`        | GET    | Retrieve quotes by a specific author (case-insensitive) | `author` (path, string)                        |

---

## 1. Get All Quotes (Paginated)

- **URL:** `/quotes`  
- **Method:** `GET`  
- **Query Parameters (optional):**
  - `page` (int) – Page number to retrieve. Defaults to 1.
  - `per_page` (int) – Number of quotes per page. Defaults to 10.

### Response Example

```json
{
  "page": 1,
  "per_page": 3,
  "quotes": [
    {
      "author": "Jerry",
      "episode": 1,
      "quote": "You look like you live with your mother!",
      "season": 1
    },
    {
      "author": "Kramer",
      "episode": 2,
      "quote": "This dictionary's no good, we need a medical dictionary! When a patient is difficult, you quone him.",
      "season": 1
    },
    {
      "author": "Elaine",
      "episode": 4,
      "quote": "You made a man cry? I've never made a man cry. I even kicked a guy in the groin once and he didn't cry.",
      "season": 1
    }
  ],
  "total_pages": 112,
  "total_quotes": 334
}

| Field          | Type  | Description                            |
| -------------- | ----- | -------------------------------------- |
| `page`         | int   | Current page number of results         |
| `per_page`     | int   | Number of quotes returned per page     |
| `quotes`       | array | List of quote objects                  |
| `total_pages`  | int   | Total number of pages available        |
| `total_quotes` | int   | Total number of quotes in the database |

| Field     | Type   | Description                  |
| --------- | ------ | ---------------------------- |
| `author`  | string | Name of the character/author |
| `season`  | int    | Season number                |
| `episode` | int    | Episode number               |
| `quote`   | string | The quote text               |

2. Get Quotes by Author

URL: /quotes/<author>

Method: GET

Path Parameter:

author (string) – Name of the character/author (case-insensitive)
[
  {
    "author": "Jerry",
    "episode": 1,
    "quote": "You look like you live with your mother!",
    "season": 1
  },
  {
    "author": "Jerry",
    "episode": 2,
    "quote": "You should do it like a band-aid -- one motion, RIGHT OFF!",
    "season": 2
  }
]

| HTTP Code | Message                           | Description                    |
| --------- | --------------------------------- | ------------------------------ |
| 500       | DB connection failed              | Cannot connect to the database |
| 500       | Error fetching quotes by <author> | Query execution failed         |

Notes

Pagination allows clients to request  subset of quotes per page.

Author search is case-insensitive.

All responses are in JSON format.

Use proper query parameters to retrieve specific pages or limit results per page.