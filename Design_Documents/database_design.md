# Database Design - Seinfeld Quote Explorer

## PostgreSQL Schema

### Characters Table
- `id` SERIAL PRIMARY KEY
- `name` VARCHAR(100) NOT NULL
- `image_url` TEXT

### Quotes Table
- `id` SERIAL PRIMARY KEY
- `character_id` INT REFERENCES characters(id)
- `quote` TEXT NOT NULL
- `season` INT
- `episode` INT
- `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP

### Relationships
- Each quote belongs to a character (`quotes.character_id` → `characters.id`)

