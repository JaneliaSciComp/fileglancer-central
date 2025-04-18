import re
from datetime import datetime

from sqlalchemy import create_engine, Column, String, Integer, DateTime, JSON, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from typing import Optional, Dict
from loguru import logger

from fileglancer_central.settings import get_settings

settings = get_settings()
Base = declarative_base()

class FileSharePathDB(Base):
    """Database model for storing file share paths"""
    __tablename__ = 'file_share_paths'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, index=True, unique=True)
    zone = Column(String)
    group = Column(String)
    storage = Column(String)
    mount_path = Column(String)
    mac_path = Column(String)
    windows_path = Column(String)
    linux_path = Column(String)
    

class LastRefreshDB(Base):
    """Database model for storing the last refresh time of the file share paths"""
    __tablename__ = 'last_refresh'
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_last_updated = Column(DateTime, nullable=False)
    db_last_updated = Column(DateTime, nullable=False)


class UserPreferenceDB(Base):
    """Database model for storing user preferences"""
    __tablename__ = 'user_preferences'

    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False)
    key = Column(String, nullable=False) 
    value = Column(JSON, nullable=False)

    __table_args__ = (
        UniqueConstraint('username', 'key', name='uq_user_pref'),
    )


def get_db_session():
    """Create and return a database session"""
    engine = create_engine(settings.db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)
    return session


def get_all_paths(session):
    """Get all file share paths from the database"""
    return session.query(FileSharePathDB).all()


def get_last_refresh(session):
    """Get the last refresh time from the database"""
    return session.query(LastRefreshDB).first()


def slugify_path(s):
    """Slugify a path to make it into a name"""
    # Replace any special characters with underscores
    s = re.sub(r'[^a-zA-Z0-9]', '_', s)
    # Replace multiple underscores with a single underscore
    s = re.sub(r'_+', '_', s)
    # Remove leading underscores
    s = s.lstrip('_')
    return s


def update_file_share_paths(session, table, table_last_updated, max_paths_to_delete=2):
    """Update database with new file share paths"""
    # Get all existing linux_paths from database
    existing_paths = {path[0] for path in session.query(FileSharePathDB.mount_path).all()}
    new_paths = set()
    num_existing = 0
    num_new = 0

    # Update or insert records
    for _, row in table.iterrows():
        mount_path = row['linux_path']
        new_paths.add(mount_path)
        
        # Check if path exists
        existing_record = session.query(FileSharePathDB).filter_by(mount_path=mount_path).first()
        
        if existing_record:
            # Update existing record
            existing_record.name = slugify_path(row['linux_path'])
            existing_record.zone = row['lab']
            existing_record.group = row['group']
            existing_record.storage = row['storage']
            existing_record.mount_path = row['linux_path']
            existing_record.mac_path = row['mac_path']
            existing_record.windows_path = row['windows_path']
            existing_record.linux_path = row['linux_path']
            
            num_existing += 1

        else:
            # Create new record
            new_record = FileSharePathDB(
                name=slugify_path(row['linux_path']),
                zone=row['lab'],
                group=row['group'],
                storage=row['storage'],
                mount_path=row['linux_path'],
                mac_path=row['mac_path'],
                windows_path=row['windows_path'],
                linux_path=row['linux_path']
            )
            session.add(new_record)
            num_new += 1

    logger.debug(f"Updated {num_existing} file share paths, added {num_new} file share paths")

    # Delete records that no longer exist in the wiki
    paths_to_delete = existing_paths - new_paths
    if paths_to_delete:
        if len(paths_to_delete) > max_paths_to_delete:
            logger.warning(f"Cannot delete {len(paths_to_delete)} defunct file share paths from the database, only {max_paths_to_delete} are allowed")
        else:
            logger.debug(f"Deleting {len(paths_to_delete)} defunct file share paths from the database")
            session.query(FileSharePathDB).filter(FileSharePathDB.linux_path.in_(paths_to_delete)).delete(synchronize_session='fetch')

    # Update last refresh time
    session.query(LastRefreshDB).delete()
    session.add(LastRefreshDB(source_last_updated=table_last_updated, db_last_updated=datetime.now()))

    session.commit()


def get_user_preference(session: Session, username: str, key: str) -> Optional[Dict]:
    """Get a user preference value by username and key"""
    pref = session.query(UserPreferenceDB).filter_by(
        username=username,
        key=key
    ).first()
    return pref.value if pref else None


def set_user_preference(session: Session, username: str, key: str, value: Dict):
    """Set a user preference value
    If the preference already exists, it will be updated with the new value.
    If the preference does not exist, it will be created.
    Returns the preference object.    
    """
    pref = session.query(UserPreferenceDB).filter_by(
        username=username, 
        key=key
    ).first()

    if pref:
        pref.value = value
    else:
        pref = UserPreferenceDB(
            username=username,
            key=key,
            value=value
        )
        session.add(pref)

    session.commit()
    return pref


def delete_user_preference(session: Session, username: str, key: str) -> bool:
    """Delete a user preference and return True if it was deleted, False if it didn't exist"""
    deleted = session.query(UserPreferenceDB).filter_by(
        username=username,
        key=key
    ).delete()
    session.commit()
    return deleted > 0


def get_all_user_preferences(session: Session, username: str) -> Dict[str, Dict]:
    """Get all preferences for a user"""
    prefs = session.query(UserPreferenceDB).filter_by(username=username).all()
    return {pref.key: pref.value for pref in prefs}


# Test harness
if __name__ == "__main__":
    session = get_db_session()
    value = {"a": 1, "b": [1, 2, 3]}
    set_user_preference(session, "tester", "favorite_color", "blue")
    set_user_preference(session, "tester", "test_key", value)
    print(get_all_user_preferences(session, "tester"))
    assert get_user_preference(session, "tester", "favorite_color") == "blue"
    assert get_user_preference(session, "tester", "test_key") == value
    delete_user_preference(session, "tester", "test_key")
    delete_user_preference(session, "tester", "favorite_color") 
    print(get_all_user_preferences(session, "tester"))
    assert get_user_preference(session, "tester", "test_key") is None
    assert get_user_preference(session, "tester", "favorite_color") is None
