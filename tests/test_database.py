import tempfile
import os
from datetime import datetime

import pytest
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fileglancer_central.database import (
    FileSharePathDB, LastRefreshDB, UserPreferenceDB,
    get_all_paths, get_last_refresh, update_file_share_paths,
    get_user_preference, set_user_preference, delete_user_preference,
    get_all_user_preferences, Base
)

@pytest.fixture
def db_session():
    """Create a test database session"""
    
    # Create temp directory for test database
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test.db")
    db_url = f"sqlite:///{db_path}"

    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)
    yield session
    # Clean up after each test
    session.query(FileSharePathDB).delete()
    session.query(LastRefreshDB).delete()
    session.query(UserPreferenceDB).delete()
    session.commit()
    session.close()


def test_file_share_paths(db_session):
    # Create test data
    data = {
        'lab': ['lab1', 'lab2'],
        'group': ['group1', 'group2'],
        'storage': ['storage1', 'storage2'],
        'linux_path': ['/path1', '/path2'],
        'mac_path': ['mac1', 'mac2'],
        'windows_path': ['win1', 'win2']
    }
    df = pd.DataFrame(data)
    
    # Test update_file_share_paths
    update_file_share_paths(db_session, df, datetime.now())
    
    # Test get_all_paths
    paths = get_all_paths(db_session)
    assert len(paths) == 2
    assert paths[0].lab == 'lab1'
    assert paths[1].lab == 'lab2'

    # Test updating existing paths
    data['lab'] = ['lab1_updated', 'lab2_updated']
    df = pd.DataFrame(data)
    update_file_share_paths(db_session, df, datetime.now())
    
    paths = get_all_paths(db_session)
    assert paths[0].lab == 'lab1_updated'
    assert paths[1].lab == 'lab2_updated'


def test_last_refresh(db_session):
    now = datetime.now()
    data = {'lab': ['lab1'], 'group': ['group1'], 'storage': ['storage1'],
            'linux_path': ['/path1'], 'mac_path': ['mac1'], 'windows_path': ['win1']}
    df = pd.DataFrame(data)
    
    update_file_share_paths(db_session, df, now)
    
    refresh = get_last_refresh(db_session)
    assert refresh is not None
    assert refresh.source_last_updated == now


def test_user_preferences(db_session):
    # Test setting preferences
    test_value = {"setting": "test"}
    set_user_preference(db_session, "testuser", "test_key", test_value)
    
    # Test getting preference
    pref = get_user_preference(db_session, "testuser", "test_key")
    assert pref == test_value
    
    # Test getting non-existent preference
    pref = get_user_preference(db_session, "testuser", "nonexistent")
    assert pref is None
    
    # Test updating preference
    new_value = {"setting": "updated"}
    set_user_preference(db_session, "testuser", "test_key", new_value)
    pref = get_user_preference(db_session, "testuser", "test_key")
    assert pref == new_value
    
    # Test getting all preferences
    all_prefs = get_all_user_preferences(db_session, "testuser")
    assert len(all_prefs) == 1
    assert all_prefs["test_key"] == new_value
    
    # Test deleting preference
    delete_user_preference(db_session, "testuser", "test_key")
    pref = get_user_preference(db_session, "testuser", "test_key")
    assert pref is None


def test_max_paths_to_delete(db_session):
    # Create initial data
    data = {
        'lab': ['lab1', 'lab2', 'lab3'],
        'group': ['group1', 'group2', 'group3'],
        'storage': ['storage1', 'storage2', 'storage3'],
        'linux_path': ['/path1', '/path2', '/path3'],
        'mac_path': ['mac1', 'mac2', 'mac3'],
        'windows_path': ['win1', 'win2', 'win3']
    }
    df = pd.DataFrame(data)
    update_file_share_paths(db_session, df, datetime.now())
    
    # Update with fewer paths (should trigger deletion limit)
    data = {
        'lab': ['lab1'],
        'group': ['group1'],
        'storage': ['storage1'],
        'linux_path': ['/path1'],
        'mac_path': ['mac1'],
        'windows_path': ['win1']
    }
    df = pd.DataFrame(data)
    
    # With max_paths_to_delete=1, should not delete paths
    update_file_share_paths(db_session, df, datetime.now(), max_paths_to_delete=1)
    paths = get_all_paths(db_session)
    assert len(paths) == 3  # Should still have all paths
