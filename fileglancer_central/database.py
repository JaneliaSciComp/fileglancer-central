
from fileglancer_central.settings import get_settings
from loguru import logger
settings = get_settings()

from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime

Base = declarative_base()

class FileSharePathDB(Base):
    __tablename__ = 'file_share_paths'
    id = Column(Integer, primary_key=True, autoincrement=True)
    lab = Column(String)
    group = Column(String, index=True)
    storage = Column(String)
    mac_path = Column(String)
    smb_path = Column(String)
    linux_path = Column(String, index=True, unique=True)
    

class LastRefreshDB(Base):
    __tablename__ = 'last_refresh'
    id = Column(Integer, primary_key=True, autoincrement=True)
    refresh_time = Column(DateTime, nullable=False)


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
    last_refresh = session.query(LastRefreshDB).first()
    if last_refresh:
        return last_refresh.refresh_time
    else:
        return None


def update_file_share_paths(session, table, max_paths_to_delete=2):
    """Update database with new file share paths"""
    # Get all existing linux_paths from database
    existing_paths = {path[0] for path in session.query(FileSharePathDB.linux_path).all()}
    new_paths = set()
    num_existing = 0
    num_new = 0

    # Update or insert records
    for _, row in table.iterrows():
        linux_path = row[table.columns[4]]
        new_paths.add(linux_path)
        
        # Check if path exists
        existing_record = session.query(FileSharePathDB).filter_by(linux_path=linux_path).first()
        

        if existing_record:
            # Update existing record
            existing_record.lab = row[table.columns[0]]
            existing_record.storage = row[table.columns[1]]
            existing_record.mac_path = row[table.columns[2]]
            existing_record.smb_path = row[table.columns[3]]
            existing_record.ad_group = row[table.columns[5]]
            num_existing += 1

        else:
            # Create new record
            new_record = FileSharePathDB(
                lab=row[table.columns[0]],
                storage=row[table.columns[1]],
                mac_path=row[table.columns[2]],
                smb_path=row[table.columns[3]],
                linux_path=linux_path,
                ad_group=row[table.columns[5]]
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
    session.add(LastRefreshDB(refresh_time=datetime.now()))

    session.commit()