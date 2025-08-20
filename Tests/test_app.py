import pytest
from app import app
from unittest.mock import patch

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

@patch("app.SeinfeldLoader")
def test_load_data_success(mock_loader, client):
    instance = mock_loader.return_value
    instance.connect_to_postgres.return_value = True
    instance.read_csv_data.return_value = [("yada yada", "Jerry", 2, 3)]
    
    response = client.post("/load")
    assert response.status_code == 200
    assert response.json["status"] == "success"

@patch("app.SeinfeldLoader")
def test_load_data_db_fail(mock_loader, client):
    instance = mock_loader.return_value
    instance.connect_to_postgres.return_value = False
    
    response = client.post("/load")
    assert response.status_code == 500
    assert response.json["status"] == "error"

@patch("app.SeinfeldLoader")
def test_get_quotes_success(mock_loader, client):
    instance = mock_loader.return_value
    instance.connect_to_postgres.return_value = True
    instance.cursor.fetchall.side_effect = [
        [("No soup for you", "Soup Nazi", 7, 6)],
        [1]  # Total count
    ]
    response = client.get("/quotes?page=1&per_page=1")
    assert response.status_code == 200
    assert response.json["quotes"][0]["author"] == "Soup Nazi"

@patch("app.SeinfeldLoader")
def test_get_quotes_by_author(mock_loader, client):
    instance = mock_loader.return_value
    instance.connect_to_postgres.return_value = True
    instance.cursor.fetchall.return_value = [("Hello Newman", "Jerry", 5, 4)]

    response = client.get("/quotes/jerry")
    assert response.status_code == 200
    assert response.json[0]["author"] == "Jerry"

@patch("app.SeinfeldLoader")
def test_get_quotes_by_author_db_fail(mock_loader, client):
    instance = mock_loader.return_value
    instance.connect_to_postgres.return_value = False
    response = client.get("/quotes/george")
    assert response.status_code == 500
    assert response.json["status"] == "error"
