import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from fileglancer_central.settings import Settings
from fileglancer_central.app import create_app

@pytest.fixture
def test_app():
    """Create test FastAPI app"""

    # Create temp directory for test database
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test.db")
    db_url = f"sqlite:///{db_path}"

    settings = Settings(db_url=db_url)
    app = create_app(settings)
    return app


@pytest.fixture
def test_client(test_app):
    """Create test client"""
    return TestClient(test_app)


def test_docs_redirect(test_client):
    """Test redirect to docs page"""
    response = test_client.get("/")
    assert response.status_code == 200
    assert str(response.url).endswith("/docs")


def test_get_preferences(test_client):
    """Test getting user preferences"""
    response = test_client.get("/preferences/testuser")
    assert response.status_code == 200
    value = response.json()
    assert isinstance(value, dict)
    assert value == {}


def test_get_specific_preference(test_client):
    """Test getting specific user preference"""
    response = test_client.get("/preferences/testuser/unknown_key")
    assert response.status_code == 404


def test_set_preference(test_client):
    """Test setting user preference"""
    pref_data = {"test": "value"}
    response = test_client.put("/preferences/testuser/test_key", json=pref_data)
    assert response.status_code == 200

    response = test_client.get("/preferences/testuser/test_key")
    assert response.status_code == 200
    assert response.json() == pref_data


def test_delete_preference(test_client):
    """Test deleting user preference"""
    response = test_client.delete("/preferences/testuser/test_key")
    assert response.status_code == 200

    response = test_client.delete("/preferences/testuser/unknown_key")
    assert response.status_code == 404
