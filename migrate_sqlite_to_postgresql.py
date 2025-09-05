#!/usr/bin/env python3
"""
SQLite to PostgreSQL Migration Script for FileGlancer Central

This script automatically discovers and migrates all tables from an existing SQLite database to PostgreSQL.
It handles schema creation and data transfer while preserving all relationships.

Features:
- Automatically discovers all user tables in the SQLite database
- Uses SQLAlchemy models when available for type-safe migration
- Falls back to raw SQL migration for tables without models
- Preserves data integrity and relationships
- Provides detailed logging and progress tracking

Usage:
    python migrate_sqlite_to_postgresql.py --sqlite-url sqlite:///fileglancer.db --postgresql-url postgresql://user:pass@host:port/dbname

Requirements:
    - Source SQLite database must exist and be accessible
    - Target PostgreSQL database must be created and accessible
    - All required Python dependencies must be installed
"""

import argparse
import sys
from typing import Optional
from datetime import datetime, UTC
import logging

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from fileglancer_central.database import Base

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def verify_database_connection(db_url: str, db_name: str) -> bool:
    """Verify database connection and basic functionality."""
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"✅ Successfully connected to {db_name} database")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to connect to {db_name} database: {e}")
        return False


def get_sqlite_tables(sqlite_url: str) -> list:
    """Get all tables that exist in SQLite database."""
    try:
        engine = create_engine(sqlite_url)
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        # Filter out system tables
        user_tables = [table for table in tables if not table.startswith('sqlite_')]

        if user_tables:
            logger.info(f"✅ Found tables in SQLite: {user_tables}")
        else:
            logger.warning(f"⚠️  No user tables found in SQLite database")

        return user_tables

    except Exception as e:
        logger.error(f"❌ Failed to inspect SQLite database: {e}")
        return []


def get_model_for_table(table_name: str):
    """Get the SQLAlchemy model class for a given table name."""
    # Get all model classes that inherit from Base
    for cls in Base.registry._class_registry.values():
        if hasattr(cls, '__tablename__') and cls.__tablename__ == table_name:
            return cls
    return None


def get_postgresql_tables(postgresql_url: str) -> list:
    """Get all tables that exist in PostgreSQL database."""
    try:
        engine = create_engine(postgresql_url)
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        if tables:
            logger.info(f"✅ Found existing tables in PostgreSQL: {tables}")
        else:
            logger.info(f"📋 No existing tables found in PostgreSQL database")

        return tables

    except Exception as e:
        logger.error(f"❌ Failed to inspect PostgreSQL database: {e}")
        return []


def table_exists_in_postgresql(postgresql_engine, table_name: str) -> bool:
    """Check if a specific table exists in PostgreSQL."""
    try:
        inspector = inspect(postgresql_engine)
        tables = inspector.get_table_names()
        return table_name in tables
    except Exception as e:
        logger.warning(f"⚠️  Could not check if table {table_name} exists: {e}")
        return False


def get_table_row_count(session, model_class) -> int:
    """Get row count for a table."""
    try:
        return session.query(model_class).count()
    except:
        return 0


def get_sqlite_table_columns(sqlite_engine, table_name: str) -> list:
    """Get the actual columns that exist in a SQLite table."""
    try:
        inspector = inspect(sqlite_engine)
        columns = inspector.get_columns(table_name)
        return [col['name'] for col in columns]
    except Exception as e:
        logger.error(f"  Failed to get columns for {table_name}: {e}")
        return []


def migrate_table_data_with_model(sqlite_session, postgresql_session, model_class, table_name: str) -> int:
    """Migrate data for a specific table."""
    try:
        # First check if we can query the table with the model
        try:
            test_query = sqlite_session.query(model_class).limit(1)
            test_query.all()  # This will fail if schema doesn't match
        except Exception as schema_error:
            logger.warning(f"  ⚠️  Schema mismatch for {table_name}: {schema_error}")
            logger.info(f"  🔄 Falling back to raw SQL migration for {table_name}")
            # Fall back to raw SQL migration
            sqlite_engine = sqlite_session.bind
            postgresql_engine = postgresql_session.bind
            return migrate_table_data_raw(sqlite_engine, postgresql_engine, table_name)

        # Get all records from SQLite
        sqlite_records = sqlite_session.query(model_class).all()
        record_count = len(sqlite_records)

        if record_count == 0:
            logger.info(f"  No data found in {table_name}")
            return 0

        logger.info(f"  Migrating {record_count} records from {table_name}")

        # Get actual columns from SQLite to avoid missing column errors
        sqlite_engine = sqlite_session.bind
        actual_columns = get_sqlite_table_columns(sqlite_engine, table_name)
        logger.info(f"  SQLite columns: {actual_columns}")

        # Convert to dictionaries and create new objects for PostgreSQL
        for record in sqlite_records:
            # Get all column values as a dictionary, but only for columns that exist
            record_dict = {}
            for column in model_class.__table__.columns:
                if column.name in actual_columns:
                    record_dict[column.name] = getattr(record, column.name)
                elif not column.nullable and column.default is None:
                    # Handle required columns that don't exist in SQLite
                    logger.warning(f"  ⚠️  Missing required column {column.name}, using None")
                    record_dict[column.name] = None

            # Create new record in PostgreSQL (excluding the id to let PostgreSQL auto-generate)
            if 'id' in record_dict:
                del record_dict['id']


            new_record = model_class(**record_dict)
            postgresql_session.add(new_record)

        postgresql_session.commit()
        logger.info(f"  ✅ Successfully migrated {record_count} records from {table_name}")
        return record_count

    except Exception as e:
        postgresql_session.rollback()
        logger.error(f"  ❌ Failed to migrate {table_name}: {e}")
        raise


def migrate_table_data_raw(sqlite_engine, postgresql_engine, table_name: str) -> int:
    """Migrate data for a table without a SQLAlchemy model using raw SQL."""
    try:
        # Check if table exists in PostgreSQL
        if not table_exists_in_postgresql(postgresql_engine, table_name):
            logger.warning(f"  ⚠️  Table {table_name} does not exist in PostgreSQL - skipping migration")
            logger.info(f"  💡 You may need to create this table manually or add it to your Alembic migrations")
            return 0

        # Get table structure from SQLite
        sqlite_inspector = inspect(sqlite_engine)
        columns = sqlite_inspector.get_columns(table_name)
        column_names = [col['name'] for col in columns]

        logger.info(f"  Found columns: {column_names}")

        # Read data from SQLite
        with sqlite_engine.connect() as sqlite_conn:
            result = sqlite_conn.execute(text(f"SELECT * FROM {table_name}"))
            rows = result.fetchall()

            if not rows:
                logger.info(f"  No data found in {table_name}")
                return 0

            logger.info(f"  Migrating {len(rows)} records from {table_name}")

        # Insert data into PostgreSQL
        with postgresql_engine.connect() as postgresql_conn:
            # Create parameterized insert statement with conflict resolution
            placeholders = ', '.join([f":{col}" for col in column_names])

            # Simple insert since tables are cleared first
            insert_sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"

            # Convert rows to dictionaries
            data_dicts = []
            for row in rows:
                row_dict = {}
                for i, col_name in enumerate(column_names):
                    row_dict[col_name] = row[i]
                data_dicts.append(row_dict)

            # Execute batch insert
            postgresql_conn.execute(text(insert_sql), data_dicts)
            postgresql_conn.commit()

        logger.info(f"  ✅ Successfully migrated {len(rows)} records from {table_name}")
        return len(rows)

    except Exception as e:
        logger.error(f"  ❌ Failed to migrate {table_name}: {e}")
        raise


def perform_migration(sqlite_url: str, postgresql_url: str, skip_existing: bool = False) -> bool:
    """Perform the complete migration from SQLite to PostgreSQL."""

    logger.info("🚀 Starting SQLite to PostgreSQL migration")

    # Verify database connections
    if not verify_database_connection(sqlite_url, "SQLite"):
        return False

    if not verify_database_connection(postgresql_url, "PostgreSQL"):
        return False

    # Get all tables from SQLite database
    sqlite_tables = get_sqlite_tables(sqlite_url)
    if not sqlite_tables:
        logger.error("❌ SQLite database appears to be empty or has no user tables")
        return False

    try:
        # Create database engines and sessions
        sqlite_engine = create_engine(sqlite_url)
        postgresql_engine = create_engine(postgresql_url)

        SqliteSession = sessionmaker(bind=sqlite_engine)
        PostgresqlSession = sessionmaker(bind=postgresql_engine)

        sqlite_session = SqliteSession()
        postgresql_session = PostgresqlSession()

        # Initialize PostgreSQL schema
        logger.info("📋 Setting up PostgreSQL schema...")
        try:
            # Use SQLAlchemy directly to create tables instead of Alembic
            # This avoids issues with Alembic reading the wrong database URL from config
            logger.info("📋 Creating tables using SQLAlchemy...")
            Base.metadata.create_all(postgresql_engine)
            logger.info("✅ PostgreSQL schema created successfully")
        except Exception as e:
            logger.warning(f"⚠️  Schema creation had issues: {e}")
            logger.info("📋 Continuing with migration - schema may already exist")

        # Show what tables exist in PostgreSQL after schema setup
        postgresql_tables = get_postgresql_tables(postgresql_url)

        # Check if PostgreSQL already has data and offer to clear it
        if not skip_existing:
            existing_data = False
            tables_with_data = []

            # Check all tables that exist in SQLite
            for table_name in sqlite_tables:
                model_class = get_model_for_table(table_name)
                if model_class:
                    count = get_table_row_count(postgresql_session, model_class)
                    if count > 0:
                        logger.warning(f"⚠️  PostgreSQL table {table_name} already contains {count} records")
                        existing_data = True
                        tables_with_data.append(table_name)
                else:
                    # For tables without models, check using raw SQL
                    try:
                        with postgresql_engine.connect() as conn:
                            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                            count = result.scalar()
                            if count > 0:
                                logger.warning(f"⚠️  PostgreSQL table {table_name} already contains {count} records")
                                existing_data = True
                                tables_with_data.append(table_name)
                    except:
                        # Table might not exist in PostgreSQL yet, which is fine
                        pass

            if existing_data:
                response = input("\n🤔 PostgreSQL database already contains data. Clear existing data before migration? (y/N): ")
                if response.lower() in ['y', 'yes']:
                    logger.info("🧹 Clearing existing data from PostgreSQL tables...")

                    # Clear tables in reverse dependency order to avoid FK issues
                    table_clear_order = ['user_preferences', 'proxied_paths', 'tickets', 'last_refresh', 'external_buckets', 'file_share_paths', 'alembic_version']

                    # Close existing sessions to avoid locks
                    sqlite_session.close()
                    postgresql_session.close()

                    for table_name in table_clear_order:
                        if table_name in tables_with_data:
                            logger.info(f"  Clearing table: {table_name}")
                            try:
                                # Create a fresh connection with autocommit to avoid hanging
                                with postgresql_engine.connect() as conn:
                                    # Set a statement timeout to prevent hanging
                                    conn.execute(text("SET statement_timeout = '10s'"))

                                    # Use DELETE instead of TRUNCATE to avoid locks
                                    result = conn.execute(text(f"DELETE FROM {table_name}"))
                                    deleted_count = result.rowcount if hasattr(result, 'rowcount') else 'unknown'

                                    # Reset sequences if they exist
                                    try:
                                        conn.execute(text(f"ALTER SEQUENCE {table_name}_id_seq RESTART WITH 1"))
                                    except:
                                        pass  # Table might not have an id sequence

                                    conn.commit()

                                logger.info(f"  ✅ Cleared table: {table_name} ({deleted_count} rows)")
                            except Exception as e:
                                logger.error(f"  ❌ Failed to clear table {table_name}: {e}")
                                return False

                    # Recreate sessions for migration
                    sqlite_session = SqliteSession()
                    postgresql_session = PostgresqlSession()

                    logger.info("✅ Data clearing completed")
                else:
                    logger.info("Migration cancelled by user")
                    return False

        # Perform migration for all discovered tables
        total_records = 0
        logger.info("📊 Starting data migration...")

        for table_name in sqlite_tables:
            logger.info(f"📋 Processing table: {table_name}")

            # Handle alembic_version specially since it doesn't have a SQLAlchemy model
            if table_name == 'alembic_version':
                logger.info(f"  Migrating Alembic version tracking table")
                # Create alembic_version table if it doesn't exist and clear any existing data
                with postgresql_engine.connect() as conn:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS alembic_version (
                            version_num VARCHAR(32) NOT NULL PRIMARY KEY
                        )
                    """))
                    # Clear any existing version data
                    conn.execute(text("DELETE FROM alembic_version"))
                    conn.commit()
                count = migrate_table_data_raw(sqlite_engine, postgresql_engine, table_name)
                total_records += count
                continue

            model_class = get_model_for_table(table_name)

            if model_class:
                logger.info(f"  Using SQLAlchemy model for {table_name}")
                count = migrate_table_data_with_model(sqlite_session, postgresql_session, model_class, table_name)
            else:
                logger.info(f"  No SQLAlchemy model found for {table_name}, using raw SQL migration")
                count = migrate_table_data_raw(sqlite_engine, postgresql_engine, table_name)

            total_records += count

        # Close sessions
        sqlite_session.close()
        postgresql_session.close()

        logger.info(f"🎉 Migration completed successfully!")
        logger.info(f"📊 Total records migrated: {total_records}")

        # Stamp the PostgreSQL database with the latest Alembic version
        logger.info("🏷️  Stamping PostgreSQL database with latest Alembic version...")
        try:
            from alembic.config import Config
            from alembic import command
            import os
            from fileglancer_central import database as db

            pkg_dir = os.path.dirname(os.path.abspath(db.__file__))
            alembic_cfg_path = None
            alembic_script_location = None

            # Check if alembic.ini is in the package directory
            pkg_alembic_cfg_path = os.path.join(pkg_dir, "alembic.ini")
            if os.path.exists(pkg_alembic_cfg_path):
                alembic_cfg_path = pkg_alembic_cfg_path
                alembic_script_location = os.path.join(pkg_dir, "alembic")
                logger.debug("Using package alembic.ini")
            else:
                # Check one level up (project root for development)
                dev_alembic_cfg_path = os.path.join(os.path.dirname(pkg_dir), "alembic.ini")
                if os.path.exists(dev_alembic_cfg_path):
                    alembic_cfg_path = dev_alembic_cfg_path
                    alembic_script_location = os.path.join(os.path.dirname(pkg_dir), "alembic")
                    logger.debug("Using development alembic.ini")

            if alembic_cfg_path and os.path.exists(alembic_cfg_path) and os.path.exists(alembic_script_location):
                alembic_cfg = Config(alembic_cfg_path)

                # Update script_location to absolute path
                alembic_cfg.set_main_option("script_location", alembic_script_location)
                logger.debug(f"Set alembic script_location to: {alembic_script_location}")

                # Always stamp with the latest version since we created all tables with SQLAlchemy
                command.stamp(alembic_cfg, "head")
                logger.info("✅ Successfully stamped database with latest migration version")
            else:
                logger.warning("⚠️  Alembic configuration not found, could not stamp database")
        except Exception as e:
            logger.warning(f"⚠️  Could not stamp database with Alembic version: {e}")
            logger.info("💡 You may need to manually run: alembic stamp head")

        logger.info(f"🔄 Next steps:")
        logger.info(f"   1. Update your config.yaml to use the PostgreSQL URL")
        logger.info(f"   2. Test your application with the new database")
        logger.info(f"   3. Backup your SQLite database for safety")

        return True

    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Migrate FileGlancer Central data from SQLite to PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_sqlite_to_postgresql.py \\
    --sqlite-url "sqlite:///fileglancer.db" \\
    --postgresql-url "postgresql://user:password@localhost:5432/fileglancer"

  python migrate_sqlite_to_postgresql.py \\
    --sqlite-url "sqlite:///database.db" \\
    --postgresql-url "postgresql://user:password@localhost:5432/fileglancer" \\
    --skip-existing-check
        """
    )

    parser.add_argument(
        "--sqlite-url",
        required=True,
        help="SQLite database URL (e.g., sqlite:///fileglancer.db)"
    )

    parser.add_argument(
        "--postgresql-url",
        required=True,
        help="PostgreSQL database URL (e.g., postgresql://user:password@localhost:5432/fileglancer)"
    )

    parser.add_argument(
        "--skip-existing-check",
        action="store_true",
        help="Skip check for existing data in PostgreSQL database"
    )

    args = parser.parse_args()

    # Perform migration
    success = perform_migration(
        args.sqlite_url,
        args.postgresql_url,
        args.skip_existing_check
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
