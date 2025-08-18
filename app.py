from flask import Flask, jsonify, request
import logging
from seinfeld_loader import SeinfeldLoader

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

CSV_FILE_PATH = "Seinfeld.csv"


@app.route("/load", methods=["POST"])
def load_data():
    loader = SeinfeldLoader(csv_file_path=CSV_FILE_PATH)
    if not loader.connect_to_postgres():
        return jsonify({"status": "error", "message": "DB connection failed"}), 500

    loader.create_table()
    data = loader.read_csv_data()
    loader.insert_data(data)
    loader.close_connection()

    return jsonify({"status": "success", "message": "CSV data loaded into database"})


@app.route("/quotes", methods=["GET"])
def get_quotes():
    loader = SeinfeldLoader(csv_file_path=CSV_FILE_PATH)
    if not loader.connect_to_postgres():
        return jsonify({"status": "error", "message": "DB connection failed"}), 500
    try:
        # pagination params (?page=1&per_page=10)
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 10))
        offset = (page - 1) * per_page

        query = """
            SELECT quote, author, season, episode
            FROM seinfeld_quotes
            ORDER BY id
            LIMIT %s OFFSET %s
        """
        loader.cursor.execute(query, (per_page, offset))
        rows = loader.cursor.fetchall()

        quotes = [
            {"quote": r[0], "author": r[1], "season": r[2], "episode": r[3]}
            for r in rows
        ]

        # get total count for pagination metadata
        loader.cursor.execute("SELECT COUNT(*) FROM seinfeld_quotes")
        total_count = loader.cursor.fetchone()[0]

        loader.close_connection()
        return jsonify({
            "page": page,
            "per_page": per_page,
            "total_quotes": total_count,
            "total_pages": (total_count + per_page - 1) // per_page,
            "quotes": quotes
        })
    except Exception as e:
        logging.error(f"Error fetching paginated quotes: {e}")
        loader.close_connection()
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/quotes/<author>", methods=["GET"])
def get_quotes_by_author(author):
    loader = SeinfeldLoader(csv_file_path=CSV_FILE_PATH)
    if not loader.connect_to_postgres():
        return jsonify({"status": "error", "message": "DB connection failed"}), 500
    try:
        query = """
            SELECT quote, author, season, episode
            FROM seinfeld_quotes
            WHERE LOWER(author) = LOWER(%s)
        """
        loader.cursor.execute(query, (author,))
        rows = loader.cursor.fetchall()
        quotes = [
            {"quote": r[0], "author": r[1], "season": r[2], "episode": r[3]}
            for r in rows
        ]
        loader.close_connection()
        return jsonify(quotes)
    except Exception as e:
        logging.error(f"Error fetching quotes by {author}: {e}")
        loader.close_connection()
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
