import tempfile
import os
import shutil
from datetime import datetime

import pytest
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fileglancer_central.database import *
from fileglancer_central.wiki import convert_table_to_file_share_paths

@pytest.fixture
def temp_dir():
    temp_dir = tempfile.mkdtemp()
    print(f"Created temp directory: {temp_dir}")
    yield temp_dir
    # Clean up the temp directory
    print(f"Cleaning up temp directory: {temp_dir}")
    shutil.rmtree(temp_dir)


@pytest.fixture
def db_session(temp_dir):
    """Create a test database session"""

    # Create temp directory for test database
    db_path = os.path.join(temp_dir, "test.db")

    engine = create_engine(f"sqlite:///{db_path}")
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


@pytest.fixture
def fsp(db_session, temp_dir):
    fsp = FileSharePathDB(
        name="tempdir", 
        zone="testzone", 
        group="testgroup", 
        storage="local", 
        mount_path=temp_dir, 
        mac_path="smb://tempdir/test/path", 
        windows_path="\\\\tempdir\\test\\path", 
        linux_path="/tempdir/test/path"
    )
    db_session.add(fsp)
    db_session.commit()
    yield fsp
    db_session.query(FileSharePathDB).delete()
    db_session.commit()
    db_session.close()


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
    paths = convert_table_to_file_share_paths(df)
    update_file_share_paths(db_session, paths, datetime.now())
    
    # Test get_all_paths
    paths = get_all_paths(db_session)
    assert len(paths) == 2
    assert paths[0].zone == 'lab1'
    assert paths[1].zone == 'lab2'

    # Test updating existing paths
    data['lab'] = ['lab1_updated', 'lab2_updated']
    df = pd.DataFrame(data)
    paths = convert_table_to_file_share_paths(df)
    update_file_share_paths(db_session, paths, datetime.now())
    
    paths = get_all_paths(db_session)
    assert paths[0].zone == 'lab1_updated'
    assert paths[1].zone == 'lab2_updated'


def test_last_refresh(db_session):
    now = datetime.now()
    data = {'lab': ['lab1'], 'group': ['group1'], 'storage': ['storage1'],
            'linux_path': ['/path1'], 'mac_path': ['mac1'], 'windows_path': ['win1']}
    df = pd.DataFrame(data)
    
    paths = convert_table_to_file_share_paths(df)
    update_file_share_paths(db_session, paths, now)
    
    refresh = get_last_refresh(db_session)
    assert refresh is not None
    assert refresh.source_last_updated == now


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
    paths = convert_table_to_file_share_paths(df)
    update_file_share_paths(db_session, paths, datetime.now())
    
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
    paths = convert_table_to_file_share_paths(df)
    # With max_paths_to_delete=1, should not delete paths
    update_file_share_paths(db_session, paths, datetime.now(), max_paths_to_delete=1)
    paths = get_all_paths(db_session)
    assert len(paths) == 3  # Should still have all paths


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


def test_create_proxied_path(db_session, fsp):
    # Test creating a new proxied path
    username = "testuser"
    sharing_name = "path"
    fsp_name = fsp.name
    path = "test/path"
    # Create temp directory in fsp_mouth_path
    test_path = os.path.join(fsp.mount_path, path)
    os.makedirs(test_path, exist_ok=True)
    proxied_path = create_proxied_path(db_session, username, sharing_name, fsp_name, path)
    
    assert proxied_path.username == username
    assert proxied_path.sharing_name == sharing_name
    assert proxied_path.sharing_key is not None


def test_get_proxied_path_by_sharing_key(db_session, fsp):
    # Test retrieving a proxied path by sharing key
    username = "testuser"
    sharing_name = "path"
    fsp_name = fsp.name
    path = "test/path"
    # Create temp directory in fsp_mouth_path
    test_path = os.path.join(fsp.mount_path, path)
    os.makedirs(test_path, exist_ok=True)
    created_path = create_proxied_path(db_session, username, sharing_name, fsp_name, path)
    
    retrieved_path = get_proxied_path_by_sharing_key(db_session, created_path.sharing_key)
    assert retrieved_path is not None
    assert retrieved_path.sharing_key == created_path.sharing_key


def test_update_proxied_path(db_session, fsp):
    # Test updating a proxied path
    username = "testuser"
    sharing_name = "path"
    fsp_name = fsp.name
    path = "test/path"
    # Create temp directory in fsp_mouth_path
    test_path = os.path.join(fsp.mount_path, path)
    os.makedirs(test_path, exist_ok=True)
    created_path = create_proxied_path(db_session, username, sharing_name, fsp_name, path)
    
    new_sharing_name = "/new/test/path"
    updated_path = update_proxied_path(db_session, username, created_path.sharing_key, new_sharing_name=new_sharing_name)
    assert updated_path.sharing_name == new_sharing_name


def test_delete_proxied_path(db_session, fsp):
    # Test deleting a proxied path
    username = "testuser"
    sharing_name = "path"
    fsp_name = fsp.name
    path = "test/path"
    # Create temp directory in fsp_mouth_path
    test_path = os.path.join(fsp.mount_path, path)
    os.makedirs(test_path, exist_ok=True)
    created_path = create_proxied_path(db_session, username, sharing_name, fsp_name, path)
    
    delete_proxied_path(db_session, username, created_path.sharing_key)
    deleted_path = get_proxied_path_by_sharing_key(db_session, created_path.sharing_key)
    assert deleted_path is None

