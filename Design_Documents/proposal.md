

Overview

A REST API and minimal web interface for browsing, searching, and getting random Seinfeld quotes by character, season, and episode.

Features

List quotes with optional filters (author, season, episode, keyword)

Get a single quote by ID

Get a random quote

Pagination support

Request logging

Swagger API documentation

Tech Stack

Python with Flask or FastAPI

MySQL or PostgreSQL database (both supported)

SQLAlchemy ORM for DB queries

Pytest for testing

Database

Table: quotes

Column

Type

Notes

id

SERIAL / INT AUTO_INCREMENT

PK

quote

TEXT

NOT NULL

author

VARCHAR(50)

NOT NULL

season

INT

NOT NULL

episode

INT

NOT NULL


API Endpoints

GET /quotes - list quotes (filters & pagination supported)

GET /quotes/:id - get quote by ID

GET /random - get a random quote

POST /quotes - add a new quote (auth required)

PUT /quotes/:id - update a quote (auth required)

DELETE /quotes/:id - delete a quote (auth required)

Getting Started

Clone the repo

Set up .env with DB credentials

Run migrations & seed CSV data using SQLAlchemy or Alembic

Start server: python app.py or uvicorn main:app --reload for FastAPI

Access API docs at /docs (Swagger/OpenAPI)

Testing

Run unit and integration tests with pytest

Optional Postman collection provided for manual testing

Wireframes

Home page: random quote with “Another Random Quote” button

Browse page: filters and paginated list of quotes

Admin page: add/edit quotes (auth required)