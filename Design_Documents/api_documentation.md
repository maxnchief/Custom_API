# API Documentation - Seinfeld Quote Explorer

## Endpoints

### GET /characters
- Returns list of characters with their image URLs

### GET /quotes?character_id=<id>
- Returns quotes for a specific character

### POST /quotes
- Add a new quote to the database
- Request body: { "character_id": int, "quote": string, "season": int, "episode": int }
