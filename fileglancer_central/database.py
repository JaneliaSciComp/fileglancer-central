import secrets
from datetime import datetime, UTC
import os

from sqlalchemy import create_engine, Column, String, Integer, DateTime, JSON, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.engine.url import make_url
from typing import Optional, Dict, List
from loguru import logger

SHARING_KEY_LENGTH = 12

# Global flag to track if migrations have been run
_migrations_run = False

# Engine cache - maintain multiple engines for different database URLs
_engine_cache = {}

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


class ExternalBucketDB(Base):
    """Database model for storing external buckets"""
    __tablename__ = 'external_buckets'
    id = Column(Integer, primary_key=True, autoincrement=True)
    full_path = Column(String)
    external_url = Column(String)
    fsp_name = Column(String, nullable=False)
    relative_path = Column(String)


class LastRefreshDB(Base):
    """Database model for storing the last refresh time of tables"""
    __tablename__ = 'last_refresh'
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String, nullable=False, index=True)
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


class ProxiedPathDB(Base):
    """Database model for storing proxied paths"""
    __tablename__ = 'proxied_paths'

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, nullable=False)
    sharing_key = Column(String, nullable=False, unique=True)
    sharing_name = Column(String, nullable=False)
    fsp_name = Column(String, nullable=False)
    path = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    __table_args__ = (
        UniqueConstraint('username', 'fsp_name', 'path', name='uq_proxied_path'),
    )


class TicketDB(Base):
    """Database model for storing proxied paths"""
    __tablename__ = 'tickets'

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, nullable=False)
    fsp_name = Column(String, nullable=False)
    path = Column(String, nullable=False)
    ticket_key = Column(String, nullable=False, unique=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    # TODO: Do we want to only allow one ticket per path?
    # Commented out now for testing purposes
    # __table_args__ = (
    #     UniqueConstraint('username', 'fsp_name', 'path', name='uq_ticket_path'),
    # )


def run_alembic_upgrade(db_url):
    """Run Alembic migrations to upgrade database to latest version"""
    global _migrations_run

    if _migrations_run:
        logger.debug("Migrations already run, skipping")
        return

    try:
        from alembic.config import Config
        from alembic import command
        import os

        alembic_cfg_path = None

        # Try to find alembic.ini - first in package directory, then development setup
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Check if alembic.ini is in the package directory (installed package)
        pkg_alembic_cfg_path = os.path.join(current_dir, "alembic.ini")
        if os.path.exists(pkg_alembic_cfg_path):
            alembic_cfg_path = pkg_alembic_cfg_path
            logger.debug("Using packaged alembic.ini")
        else:
            # Fallback to development setup
            project_root = os.path.dirname(current_dir)
            dev_alembic_cfg_path = os.path.join(project_root, "alembic.ini")
            if os.path.exists(dev_alembic_cfg_path):
                alembic_cfg_path = dev_alembic_cfg_path
                logger.debug("Using development alembic.ini")

        if alembic_cfg_path and os.path.exists(alembic_cfg_path):
            alembic_cfg = Config(alembic_cfg_path)
            alembic_cfg.set_main_option("sqlalchemy.url", db_url)

            # Update script_location for packaged installations
            if alembic_cfg_path == pkg_alembic_cfg_path:
                # Using packaged alembic.ini, also update script_location
                pkg_alembic_dir = os.path.join(current_dir, "alembic")
                if os.path.exists(pkg_alembic_dir):
                    alembic_cfg.set_main_option("script_location", pkg_alembic_dir)

            command.upgrade(alembic_cfg, "head")
            logger.info("Alembic migrations completed successfully")
        else:
            logger.warning("Alembic configuration not found, falling back to create_all")
            engine = _get_engine(db_url)
            Base.metadata.create_all(engine)
    except Exception as e:
        logger.warning(f"Alembic migration failed, falling back to create_all: {e}")
        engine = _get_engine(db_url)
        Base.metadata.create_all(engine)
    finally:
        _migrations_run = True


def initialize_database(db_url):
    """Initialize database by running migrations. Should be called once at startup."""
    run_alembic_upgrade(db_url)


def _get_engine(db_url):
    """Get or create a cached database engine for the given URL"""
    global _engine_cache

    # Return cached engine if it exists
    if db_url in _engine_cache:
        return _engine_cache[db_url]

    url = make_url(db_url)
    if url.drivername.startswith("sqlite"):
        if url.database in (None, "", ":memory:"):
            logger.warning("Configuring in-memory SQLite. This is not recommended for production use. Make sure to use --workers 1 when running uvicorn.")
            engine = create_engine(
                db_url,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool
            )
            _engine_cache[db_url] = engine
            return engine

        # File-based SQLite
        engine = create_engine(
            db_url,
            connect_args={"check_same_thread": False},  # Needed for SQLite with multiple threads
        )
        _engine_cache[db_url] = engine
        return engine

    # For other databases, use connection pooling options
    # Create new engine and cache it
    engine = create_engine(
        db_url,
        pool_size=10,
        max_overflow=20,
        pool_recycle=3600,  # Recycle connections after 1 hour
        pool_pre_ping=True  # Verify connections before use
    )
    _engine_cache[db_url] = engine
    return engine

def get_db_session(db_url):
    """Create and return a database session using a cached engine"""
    engine = _get_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    return session


def dispose_engine(db_url=None):
    """Dispose of cached engine(s) and close connections"""
    global _engine_cache

    if db_url is None:
        # Dispose all engines
        for engine in _engine_cache.values():
            engine.dispose()
        _engine_cache.clear()
    elif db_url in _engine_cache:
        # Dispose specific engine
        _engine_cache[db_url].dispose()
        del _engine_cache[db_url]


def get_all_paths(session):
    """Get all file share paths from the database"""
    return session.query(FileSharePathDB).all()


def get_all_external_buckets(session):
    """Get all external buckets from the database"""
    return session.query(ExternalBucketDB).all()


def get_last_refresh(session, table_name: str):
    """Get the last refresh time from the database for a specific table"""
    return session.query(LastRefreshDB).filter_by(table_name=table_name).first()


def update_file_share_paths(session, paths, table_last_updated, max_paths_to_delete=2):
    """Update database with new file share paths"""
    # Get all existing linux_paths from database
    existing_paths = {path[0] for path in session.query(FileSharePathDB.mount_path).all()}
    new_paths = set()
    num_existing = 0
    num_new = 0

    # Update or insert records
    for path_dict in paths:
        mount_path = path_dict['mount_path']
        new_paths.add(mount_path)

        # Check if path exists
        existing_record = session.query(FileSharePathDB).filter_by(mount_path=mount_path).first()

        if existing_record:
            # Update existing record
            existing_record.name = path_dict['name']
            existing_record.zone = path_dict['zone']
            existing_record.group = path_dict['group']
            existing_record.storage = path_dict['storage']
            existing_record.mount_path = path_dict['mount_path']
            existing_record.mac_path = path_dict['mac_path']
            existing_record.windows_path = path_dict['windows_path']
            existing_record.linux_path = path_dict['linux_path']
            num_existing += 1
        else:
            # Create new record from dictionary
            new_path = FileSharePathDB(
                name=path_dict['name'],
                zone=path_dict['zone'],
                group=path_dict['group'],
                storage=path_dict['storage'],
                mount_path=path_dict['mount_path'],
                mac_path=path_dict['mac_path'],
                windows_path=path_dict['windows_path'],
                linux_path=path_dict['linux_path']
            )
            session.add(new_path)
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
    table_name = "file_share_paths"
    session.query(LastRefreshDB).filter_by(table_name=table_name).delete()
    session.add(LastRefreshDB(table_name=table_name, source_last_updated=table_last_updated, db_last_updated=datetime.now(UTC)))

    session.commit()



def update_external_buckets(session, buckets, table_last_updated):
    """Update database with new external buckets"""
    # Get all file share paths to determine fsp_name and relative_path
    all_fsp = session.query(FileSharePathDB).all()

    # Get all existing external buckets from database
    existing_buckets = {bucket[0] for bucket in session.query(ExternalBucketDB.full_path).all()}
    new_buckets = set()
    num_existing = 0
    num_new = 0

    # Update or insert records
    for bucket_dict in buckets:
        full_path = bucket_dict['full_path']
        external_url = bucket_dict['external_url']
        new_buckets.add(full_path)

        # Determine fsp_name and relative_path by finding matching FileSharePathDB
        fsp_name = None
        relative_path = None

        for fsp in all_fsp:
            if full_path.startswith(fsp.mount_path):
                fsp_name = fsp.name
                # Remove the mount_path prefix and any leading slash
                relative_path = full_path[len(fsp.mount_path):].lstrip('/')
                break

        if fsp_name is None:
            logger.warning(f"Could not find matching file share path for external bucket: {full_path}")
            continue  # Skip buckets without matching file share paths

        # Check if bucket exists
        existing_record = session.query(ExternalBucketDB).filter_by(full_path=full_path).first()

        if existing_record:
            # Update existing record
            existing_record.external_url = external_url
            existing_record.fsp_name = fsp_name
            existing_record.relative_path = relative_path
            num_existing += 1
        else:
            # Create new record with determined fsp_name and relative_path
            new_bucket = ExternalBucketDB(
                full_path=full_path,
                external_url=external_url,
                fsp_name=fsp_name,
                relative_path=relative_path
            )
            session.add(new_bucket)
            num_new += 1

    logger.debug(f"Updated {num_existing} external buckets, added {num_new} external buckets")

    # Delete records that no longer exist
    buckets_to_delete = existing_buckets - new_buckets
    if buckets_to_delete:
        logger.debug(f"Deleting {len(buckets_to_delete)} defunct external buckets from the database")
        session.query(ExternalBucketDB).filter(ExternalBucketDB.full_path.in_(buckets_to_delete)).delete(synchronize_session='fetch')

    # Update last refresh time
    table_name = "external_buckets"
    session.query(LastRefreshDB).filter_by(table_name=table_name).delete()
    session.add(LastRefreshDB(table_name=table_name, source_last_updated=table_last_updated, db_last_updated=datetime.now(UTC)))

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


def get_proxied_paths(session: Session, username: str, fsp_name: str = None, path: str = None) -> List[ProxiedPathDB]:
    """Get proxied paths for a user, optionally filtered by fsp_name and path"""
    logger.info(f"Getting proxied paths for {username} with fsp_name={fsp_name} and path={path}")
    query = session.query(ProxiedPathDB).filter_by(username=username)
    if fsp_name:
        query = query.filter_by(fsp_name=fsp_name)
    if path:
        query = query.filter_by(path=path)
    return query.all()


def get_proxied_path_by_sharing_key(session: Session, sharing_key: str) -> Optional[ProxiedPathDB]:
    """Get a proxied path by sharing key"""
    return session.query(ProxiedPathDB).filter_by(sharing_key=sharing_key).first()


def _validate_proxied_path(session: Session, fsp_name: str, path: str) -> None:
    """Validate a proxied path exists and is accessible"""
    # Validate that the fsp_name exists in file_share_paths
    fsp = session.query(FileSharePathDB).filter_by(name=fsp_name).first()
    if not fsp:
        raise ValueError(f"File share path {fsp_name} does not exist")

    # Validate path exists and is accessible
    absolute_path = os.path.join(fsp.mount_path, path.lstrip('/'))
    try:
        os.listdir(absolute_path)
    except FileNotFoundError:
        raise ValueError(f"Path {path} does not exist relative to {fsp_name}")
    except PermissionError:
        raise ValueError(f"Path {path} is not accessible relative to {fsp_name}")


def create_proxied_path(session: Session, username: str, sharing_name: str, fsp_name: str, path: str) -> ProxiedPathDB:
    """Create a new proxied path"""
    _validate_proxied_path(session, fsp_name, path)

    sharing_key = secrets.token_urlsafe(SHARING_KEY_LENGTH)
    now = datetime.now(UTC)
    session.add(ProxiedPathDB(
        username=username,
        sharing_key=sharing_key,
        sharing_name=sharing_name,
        fsp_name=fsp_name,
        path=path,
        created_at=now,
        updated_at=now
    ))
    session.commit()
    return get_proxied_path_by_sharing_key(session, sharing_key)


def update_proxied_path(session: Session,
                        username: str,
                        sharing_key: str,
                        new_sharing_name: Optional[str] = None,
                        new_path: Optional[str] = None,
                        new_fsp_name: Optional[str] = None) -> ProxiedPathDB:
    """Update a proxied path"""
    proxied_path = get_proxied_path_by_sharing_key(session, sharing_key)
    if not proxied_path:
        raise ValueError(f"Proxied path with sharing key {sharing_key} not found")

    if username != proxied_path.username:
        raise ValueError(f"Proxied path with sharing key {sharing_key} not found for user {username}")

    if new_sharing_name:
        proxied_path.sharing_name = new_sharing_name

    if new_fsp_name:
        proxied_path.fsp_name = new_fsp_name

    if new_path:
        proxied_path.path = new_path

    _validate_proxied_path(session, proxied_path.fsp_name, proxied_path.path)

    session.commit()
    return proxied_path


def delete_proxied_path(session: Session, username: str, sharing_key: str):
    """Delete a proxied path"""
    session.query(ProxiedPathDB).filter_by(username=username, sharing_key=sharing_key).delete()
    session.commit()


def get_tickets(session: Session, username: str, fsp_name: str = None, path: str = None) -> List[TicketDB]:
    """Get tickets for a user, optionally filtered by fsp_name and path"""
    logger.info(f"Getting tickets for {username} with fsp_name={fsp_name} and path={path}")
    query = session.query(TicketDB).filter_by(username=username)
    if fsp_name:
        query = query.filter_by(fsp_name=fsp_name)
    if path:
        query = query.filter_by(path=path)
    return query.all()


def create_ticket_entry(session: Session, username: str, fsp_name: str, path: str, ticket_key: str) -> TicketDB:
    """Create a new ticket entry in the database"""
    now = datetime.now(UTC)
    ticket = TicketDB(
        username=username,
        fsp_name=fsp_name,
        path=path,
        ticket_key=ticket_key,
        created_at=now,
        updated_at=now
    )
    session.add(ticket)
    session.commit()
    return ticket
