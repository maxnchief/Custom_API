# Custom_API – Seinfeld Quotes API

A simple full-stack project to serve Seinfeld quotes via a Flask API and display them on a frontend using plain HTML/JS. Includes logging, pagination, and API documentation.

---

## 🗂 Project Structure

Custom_API/
├─ app.py
├─ seinfeld_loader.py
├─ Seinfeld.csv
├─ schema.sql
├─ quotes.sql
├─ FrontEnd/
│ ├─ index.html
│ └─ images/
├─ Design_Documents/
│ ├─ api_documentation.md
│ ├─ database_design.md
│ ├─ proposal.md
│ └─ wireframes/
│ └─ wireframe1.png
│ └─ wireframe2.png
├─ tests/
├─ venv/
├─ README.md


---

## 📊 Database Design

Here’s the ERD for the Seinfeld quotes database:

![ERD Diagram](Design_Documents/images/ERD_Diagram.png)

> This shows the `seinfeld_quotes` table and its columns: `id`, `quote`, `author`, `season`, and `episode`.

---

## 🖼 Wireframes

Wireframes show the intended layout of the frontend page(s):

### Homepage

![Homepage Wireframe](Design_Documents/wireframes/homepage.png)

### Quotes View

![Quotes Wireframe](Design_Documents/wireframes/quotes_view.png)

---

## 🛠 Database Setup

1. Install PostgreSQL (if not already installed).

2. Create the database:

```bash
createdb seinfeld

Run the SQL schema script:
psql -U your_username -d seinfeld -f schema.sql

Load data from the CSV file:

In the psql terminal:
\copy seinfeld_quotes(quote, author, season, episode) FROM 'Seinfeld.csv' CSV HEADER;

🖥 Backend Setup

Activate the virtual environment:
source venv/bin/activate

Install dependencies:

pip install -r requirements.txt


Run the Flask API server:

python3 app.py


Backend runs at http://localhost:5000.

🌐 Frontend Setup

Open a separate terminal, navigate to the FrontEnd folder:

cd FrontEnd


Start a simple HTTP server:

python3 -m http.server 8000


Open your browser at http://localhost:8000.

Frontend communicates with the backend API (localhost:5000) using CORS.

🔗 API Endpoints

Get all quotes with pagination

GET /quotes?page=1&per_page=10

Response:

{
  "page": 1,
  "per_page": 3,
  "total_quotes": 334,
  "total_pages": 112,
  "quotes": [
    {
      "author": "Jerry",
      "season": 1,
      "episode": 1,
      "quote": "You look like you live with your mother!"
    },
    ...
  ]
}


Get quotes by author

GET /quotes/<author> (case-insensitive)

Response Example:

[
  {
    "author": "Jerry",
    "season": 1,
    "episode": 1,
    "quote": "You look like you live with your mother!"
  },
  ...
]

🧪 Testing

Run pytest:

pytest -v


Tests cover:

/quotes endpoint

/quotes/<author> endpoint

JSON response structure and success cases

📝 Features Implemented

Logger (Flask + Python logging)

Pagination on /quotes

API documentation via Sphinx

Unit tests with pytest

Flask-CORS enabled for frontend communication

⚡ Future Improvements

User authentication

Caching responses for faster performance

Web sockets for live quote updates

Queuing system for long-running background processes