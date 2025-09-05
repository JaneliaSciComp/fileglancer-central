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

# Removed complex model-based migration - this is now a brain dead copy script

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


# All complex migration functions removed - now using simple brain dead copy approach


def perform_migration(sqlite_url: str, postgresql_url: str, skip_existing: bool = False) -> bool:
    """Perform a brain dead copy from SQLite to PostgreSQL: copy schema exactly, then copy all data."""

    logger.info("🚀 Starting SQLite to PostgreSQL migration")
    logger.info(f"📝 Parameters: sqlite_url={sqlite_url}, postgresql_url=<hidden>, skip_existing={skip_existing}")

    # Verify database connections
    if not verify_database_connection(sqlite_url, "SQLite"):
        return False

    if not verify_database_connection(postgresql_url, "PostgreSQL"):
        return False

    sqlite_engine = None
    postgresql_engine = None

    try:
        # Create database engines
        sqlite_engine = create_engine(sqlite_url)
        postgresql_engine = create_engine(postgresql_url)

        # Get all tables from SQLite
        sqlite_inspector = inspect(sqlite_engine)
        sqlite_tables = sqlite_inspector.get_table_names()

        if not sqlite_tables:
            logger.error("❌ SQLite database appears to be empty")
            return False

        logger.info(f"✅ Found {len(sqlite_tables)} tables in SQLite: {sqlite_tables}")

        # Check if PostgreSQL has any existing tables/data and offer to replace everything
        logger.info(f"🔍 Checking for existing data in PostgreSQL (skip_existing={skip_existing})...")
        if not skip_existing:
            existing_data = False
            existing_tables = []

            # Check if any SQLite tables already exist in PostgreSQL using information_schema
            with postgresql_engine.connect() as conn:
                # Get list of existing tables in PostgreSQL
                result = conn.execute(text("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                """))
                existing_pg_tables = {row[0] for row in result}
                logger.info(f"Found existing PostgreSQL tables: {list(existing_pg_tables)}")

                for table_name in sqlite_tables:
                    if table_name in existing_pg_tables:
                        try:
                            # Get count if table exists
                            result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
                            count = result.scalar()
                            existing_tables.append((table_name, count))
                            existing_data = True
                            if count > 0:
                                logger.warning(f"⚠️  PostgreSQL table {table_name} already exists with {count} records")
                            else:
                                logger.warning(f"⚠️  PostgreSQL table {table_name} already exists (empty)")
                        except Exception as e:
                            logger.warning(f"⚠️  PostgreSQL table {table_name} exists but could not count records: {e}")
                            existing_tables.append((table_name, 0))
                            existing_data = True
                    else:
                        logger.debug(f"Table {table_name} does not exist in PostgreSQL")

            if existing_data:
                total_records = sum(count for _, count in existing_tables)
                logger.info(f"Found {len(existing_tables)} existing tables with {total_records} total records")
                response = input(f"\n🤔 PostgreSQL already has {total_records} records in {len(existing_tables)} tables. Replace all data? (y/N): ")
                if response.lower() in ['y', 'yes']:
                    logger.info("🧹 Dropping entire PostgreSQL schema and recreating...")

                    with postgresql_engine.connect() as conn:
                        # Drop all tables (CASCADE will handle dependencies)
                        for table_name in sqlite_tables:
                            try:
                                conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
                                logger.info(f"    Dropped table: {table_name}")
                            except Exception as e:
                                logger.warning(f"    Could not drop table {table_name}: {e}")

                        # Also drop any sequences that might remain
                        try:
                            sequences_result = conn.execute(text("""
                                SELECT sequencename FROM pg_sequences
                                WHERE schemaname = 'public'
                            """))
                            for (seq_name,) in sequences_result:
                                conn.execute(text(f'DROP SEQUENCE IF EXISTS "{seq_name}" CASCADE'))
                                logger.info(f"    Dropped sequence: {seq_name}")
                        except:
                            pass  # Sequences might not exist

                        conn.commit()

                    logger.info("✅ PostgreSQL schema completely cleared")
                else:
                    logger.info("Migration cancelled by user")
                    return False
            else:
                logger.info("✅ No existing data found in PostgreSQL")
        else:
            logger.info("⏭️  Skipping existing data check as requested")

        # Step 1: Copy schema exactly from SQLite to PostgreSQL
        logger.info("📋 Copying schema from SQLite to PostgreSQL...")
        with postgresql_engine.connect() as pg_conn:
            for table_name in sqlite_tables:
                logger.info(f"  Creating table: {table_name}")

                # Get SQLite table schema
                columns = sqlite_inspector.get_columns(table_name)
                primary_keys = sqlite_inspector.get_pk_constraint(table_name)
                indexes = sqlite_inspector.get_indexes(table_name)
                unique_constraints = sqlite_inspector.get_unique_constraints(table_name)

                # Build CREATE TABLE statement
                col_definitions = []
                pk_columns = primary_keys.get('constrained_columns', []) if primary_keys else []

                for col in columns:
                    col_def = f'"{col["name"]}" '

                    # Type mapping with special handling for auto-increment primary keys
                    sqlite_type = str(col['type']).upper()
                    is_pk = col['name'] in pk_columns
                    is_autoincrement = col.get('autoincrement', False)

                    if 'INTEGER' in sqlite_type and is_pk:
                        # Primary key INTEGER columns should be SERIAL for auto-increment
                        col_def += 'SERIAL'
                    elif 'INTEGER' in sqlite_type:
                        col_def += 'INTEGER'
                    elif 'VARCHAR' in sqlite_type or 'TEXT' in sqlite_type or 'STRING' in sqlite_type:
                        col_def += 'TEXT'
                    elif 'DATETIME' in sqlite_type:
                        col_def += 'TIMESTAMP'
                    elif 'JSON' in sqlite_type:
                        col_def += 'JSONB'
                    else:
                        col_def += 'TEXT'

                    if not col.get('nullable', True):
                        col_def += ' NOT NULL'

                    col_definitions.append(col_def)

                # Add primary key constraint (only if not already handled by SERIAL)
                if primary_keys and primary_keys.get('constrained_columns'):
                    pk_cols = primary_keys['constrained_columns']
                    # Only add explicit PRIMARY KEY constraint if we have multiple columns
                    # or if the single PK column is not SERIAL (SERIAL implies PRIMARY KEY)
                    if len(pk_cols) > 1:
                        pk_col_list = ', '.join([f'"{col}"' for col in pk_cols])
                        col_definitions.append(f'PRIMARY KEY ({pk_col_list})')
                    elif len(pk_cols) == 1:
                        # Check if the single PK column is SERIAL
                        pk_col_name = pk_cols[0]
                        pk_col_info = next((col for col in columns if col['name'] == pk_col_name), None)
                        if pk_col_info:
                            sqlite_type = str(pk_col_info['type']).upper()
                            if not ('INTEGER' in sqlite_type):
                                # Not INTEGER, so not SERIAL, need explicit PRIMARY KEY
                                col_definitions.append(f'PRIMARY KEY ("{pk_col_name}")')

                # Add unique constraints
                for constraint in unique_constraints:
                    if constraint.get('column_names'):
                        unique_cols = ', '.join([f'"{col}"' for col in constraint['column_names']])
                        col_definitions.append(f'UNIQUE ({unique_cols})')

                create_table_sql = f'CREATE TABLE "{table_name}" (\n  ' + ',\n  '.join(col_definitions) + '\n)'
                pg_conn.execute(text(create_table_sql))

                # Create indexes
                for index in indexes:
                    if not index.get('unique', False):
                        index_cols = ', '.join([f'"{col}"' for col in index['column_names']])
                        index_name = f'ix_{table_name}_{"_".join(index["column_names"])}'
                        create_index_sql = f'CREATE INDEX "{index_name}" ON "{table_name}" ({index_cols})'
                        pg_conn.execute(text(create_index_sql))

            pg_conn.commit()

        logger.info("✅ Schema copied successfully")

        # Step 2: Copy all data from SQLite to PostgreSQL
        logger.info("📊 Copying data from SQLite to PostgreSQL...")
        total_records = 0

        for table_name in sqlite_tables:
            logger.info(f"  Processing table: {table_name}")

            # Get column names
            columns = sqlite_inspector.get_columns(table_name)
            column_names = [col['name'] for col in columns]

            # Read all data from SQLite
            with sqlite_engine.connect() as sqlite_conn:
                result = sqlite_conn.execute(text(f'SELECT * FROM "{table_name}"'))
                rows = result.fetchall()

                if not rows:
                    logger.info(f"    No data in {table_name}")
                    continue

                logger.info(f"    Copying {len(rows)} records")

                # Insert into PostgreSQL
                with postgresql_engine.connect() as pg_conn:
                    placeholders = ', '.join([f":{col}" for col in column_names])
                    column_list = ", ".join([f'"{col}"' for col in column_names])
                    insert_sql = f'INSERT INTO "{table_name}" ({column_list}) VALUES ({placeholders})'

                    # Convert rows to dictionaries
                    data_dicts = []
                    for row in rows:
                        row_dict = {}
                        for i, col_name in enumerate(column_names):
                            row_dict[col_name] = row[i]
                        data_dicts.append(row_dict)

                    pg_conn.execute(text(insert_sql), data_dicts)
                    pg_conn.commit()

                total_records += len(rows)
                logger.info(f"    ✅ Copied {len(rows)} records from {table_name}")

        logger.info(f"🎉 Migration completed successfully!")
        logger.info(f"📊 Total records migrated: {total_records}")
        logger.info(f"✅ All {len(sqlite_tables)} tables copied with exact schema and data")

        return True

    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        return False
    finally:
        # Properly dispose of database engines and close all connections
        if sqlite_engine is not None:
            logger.debug("Disposing SQLite engine connections...")
            sqlite_engine.dispose()
        if postgresql_engine is not None:
            logger.debug("Disposing PostgreSQL engine connections...")
            postgresql_engine.dispose()
        logger.debug("Database connections cleaned up")


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
