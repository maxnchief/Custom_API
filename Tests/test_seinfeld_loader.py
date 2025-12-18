import sys
import os
import pytest
from unittest.mock import patch

# Add project root to sys.path so Python can find app.py
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import app

# ----------------------------
# Fixture for flask test client
# ----------------------------
@pytest.fixture
def client():
    app.testing = True
    with app.test_client() as client:
        yield client

# ----------------------------
# Test /quotes endpoint (real DB connection)
# ----------------------------
def test_get_quotes(client):
    response = client.get("/quotes")
    assert response.status_code == 200
    data = response.get_json()
    assert "quotes" in data
    assert isinstance(data["quotes"], list)

# ----------------------------
# Test /quotes/<author> endpoint (real DB connection)
# ----------------------------
def test_get_quotes_by_author(client):
    author_name = "jerry"
    response = client.get(f"/quotes/{author_name}")
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    if data:
        assert all(d["author"].lower() == author_name for d in data)

# ----------------------------
# Simulate DB connection failure for /quotes
# ----------------------------
def test_get_quotes_db_failure(client):
    with patch("app.SeinfeldLoader.connect_to_postgres", return_value=False):
        response = client.get("/quotes")
        assert response.status_code == 500
        data = response.get_json()
        assert data["status"] == "error"
        assert "DB connection failed" in data["message"]

# ----------------------------
# Simulate DB connection failure for /quotes/<author>
# ----------------------------
def test_get_quotes_by_author_db_failure(client):
    with patch("app.SeinfeldLoader.connect_to_postgres", return_value=False):
        response = client.get("/quotes/Jerry")
        assert response.status_code == 500
        data = response.get_json()
        assert data["status"] == "error"
        assert "DB connection failed" in data["message"]

# ----------------------------
# Simulate successful DB call with mock data
# ----------------------------
def test_get_quotes_success_mock(client):
    mock_data = [
        ("Hello world!", "Jerry", 1, 1),
        ("Hi there!", "Elaine", 1, 2)
    ]

    class MockCursor:
        def execute(self, *args, **kwargs):
            pass
        def fetchall(self):
            return mock_data
        def fetchone(self):
            return [len(mock_data)]

    class MockLoader:
        cursor = MockCursor()
        def connect_to_postgres(self): return True
        def close_connection(self): pass

    with patch("app.SeinfeldLoader", return_value=MockLoader()):
        response = client.get("/quotes")
        assert response.status_code == 200
        data = response.get_json()
        assert data["total_quotes"] == 2
        assert len(data["quotes"]) == 2
        assert data["quotes"][0]["author"] == "Jerry"