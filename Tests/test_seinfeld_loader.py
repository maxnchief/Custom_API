import pytest
from unittest.mock import patch, MagicMock
from seinfeld_loader import SeinfeldLoader

CSV_PATH = "tests/fake.csv"

@patch("seinfeld_loader.psycopg2.connect")
def test_connect_to_postgres_success(mock_connect):
    loader = SeinfeldLoader(CSV_PATH)
    mock_connect.return_value.cursor.return_value = MagicMock()
    assert loader.connect_to_postgres() is True

@patch("seinfeld_loader.psycopg2.connect", side_effect=Exception("fail"))
def test_connect_to_postgres_fail(mock_connect):
    loader = SeinfeldLoader(CSV_PATH)
    assert loader.connect_to_postgres() is False

@patch("builtins.open")
@patch("csv.reader")
def test_read_csv_data_valid(mock_csv_reader, mock_open):
    loader = SeinfeldLoader(CSV_PATH)
    mock_csv_reader.return_value = iter([
        ["quote", "author", "season", "episode"],
        ["No soup for you", "Soup Nazi", "7", "6"]
    ])
    result = loader.read_csv_data()
    assert result == [("No soup for you", "Soup Nazi", 7, 6)]

def test_insert_data_empty():
    loader = SeinfeldLoader(CSV_PATH)
    assert loader.insert_data([]) is False

@patch("seinfeld_loader.psycopg2.connect")
def test_create_table_success(mock_connect):
    loader = SeinfeldLoader(CSV_PATH)
    mock_conn = mock_connect.return_value
    loader.connect_to_postgres()
    result = loader.create_table()
    assert result is True
