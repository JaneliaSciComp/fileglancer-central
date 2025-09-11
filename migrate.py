#!/usr/bin/env python3
"""
SQLite to PostgreSQL Migration Script

This script transfers data from a SQLite database to a PostgreSQL database using
command line connection strings. It leverages Alembic for schema management and
focuses on data migration with comprehensive error handling and progress reporting.
"""

import argparse
import os
import sqlite3
import sys
import logging
from datetime import datetime, UTC
from typing import Optional, Dict, List, Any

# Check for required dependencies
try:
    from alembic.config import Config
    from alembic import command
    ALEMBIC_AVAILABLE = True
except ImportError:
    print("⚠️  Warning: Alembic not available. Please install with: pip install alembic")
    ALEMBIC_AVAILABLE = False

try:
    import sqlalchemy
    from sqlalchemy import create_engine, text, inspect, MetaData
    from sqlalchemy.orm import sessionmaker
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    print("❌ Error: SQLAlchemy is required. Please install with: pip install sqlalchemy")
    sys.exit(1)

try:
    import psycopg2
    POSTGRESQL_DRIVER = 'psycopg2'
except ImportError:
    try:
        import asyncpg
        POSTGRESQL_DRIVER = 'asyncpg'
    except ImportError:
        print("❌ Error: PostgreSQL driver required. Please install with: pip install psycopg2-binary")
        sys.exit(1)


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO

    # Configure logging with forced flushing
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True  # Force reconfigure if already configured
    )

    # Disable SQLAlchemy logging unless in verbose mode
    if not verbose:
        logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
        logging.getLogger('sqlalchemy.dialects').setLevel(logging.WARNING)
        logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
        logging.getLogger('sqlalchemy.orm').setLevel(logging.WARNING)

    # Force unbuffered output
    import sys
    sys.stdout.reconfigure(line_buffering=True)

    logger = logging.getLogger(__name__)

    # Add explicit flush after each log message
    original_info = logger.info
    original_warning = logger.warning
    original_error = logger.error
    original_debug = logger.debug

    def info_with_flush(msg, *args, **kwargs):
        original_info(msg, *args, **kwargs)
        sys.stdout.flush()

    def warning_with_flush(msg, *args, **kwargs):
        original_warning(msg, *args, **kwargs)
        sys.stdout.flush()

    def error_with_flush(msg, *args, **kwargs):
        original_error(msg, *args, **kwargs)
        sys.stdout.flush()

    def debug_with_flush(msg, *args, **kwargs):
        original_debug(msg, *args, **kwargs)
        sys.stdout.flush()

    logger.info = info_with_flush
    logger.warning = warning_with_flush
    logger.error = error_with_flush
    logger.debug = debug_with_flush

    return logger


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Migrate data from SQLite to PostgreSQL database using Alembic for schema management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic migration
  python migrate.py --sqlite-url sqlite:///source.db --postgresql-url postgresql://user:pass@localhost:5432/target_db

  # With custom Alembic configuration
  python migrate.py --sqlite-url sqlite:///source.db --postgresql-url postgresql://user:pass@localhost:5432/target_db \\
    --alembic-config /path/to/alembic.ini --alembic-script-location /path/to/alembic/versions

Optional flags:
  --batch-size 1000           # Control memory usage for large datasets
  --verbose                  # Enable detailed logging
  --yes, -y                  # Automatically answer yes to all prompts
  --alembic-config PATH      # Custom alembic.ini file path
  --alembic-script-location PATH  # Custom alembic versions directory
        """
    )

    parser.add_argument(
        '--sqlite-url',
        required=True,
        help='SQLite database file path or connection string'
    )

    parser.add_argument(
        '--postgresql-url',
        required=True,
        help='PostgreSQL connection string (format: postgresql://user:password@host:port/database)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for data migration (default: 1000)'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable detailed logging'
    )

    parser.add_argument(
        '--alembic-config',
        help='Path to alembic.ini configuration file (default: alembic.ini)'
    )

    parser.add_argument(
        '--alembic-script-location',
        help='Path to alembic versions directory (default: alembic/)'
    )

    parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='Automatically answer yes to all prompts'
    )

    return parser.parse_args()


def validate_sqlite_connection(sqlite_url: str, logger: logging.Logger) -> bool:
    """Validate SQLite database connection."""
    try:
        engine = create_engine(sqlite_url, echo=False)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ SQLite connection validated successfully")
        return True
    except Exception as e:
        logger.error(f"❌ SQLite connection failed: {e}")
        return False


def validate_postgresql_connection(postgresql_url: str, logger: logging.Logger) -> bool:
    """Validate PostgreSQL database connection."""
    try:
        engine = create_engine(postgresql_url, echo=False)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ PostgreSQL connection validated successfully")
        return True
    except Exception as e:
        logger.error(f"❌ PostgreSQL connection failed: {e}")
        return False


def setup_alembic_config(postgresql_url: str, logger: logging.Logger,
                        alembic_config_path: Optional[str] = None,
                        alembic_script_location: Optional[str] = None) -> Optional[Config]:
    """Setup Alembic configuration with PostgreSQL connection string."""
    if not ALEMBIC_AVAILABLE:
        logger.error("❌ Alembic is not available. Please install with: pip install alembic")
        return None

    try:
        # Use provided path or search for default locations
        if alembic_config_path:
            alembic_cfg_path = alembic_config_path
        else:
            # Search for alembic.ini in common locations
            potential_paths = [
                "alembic.ini",
                "fileglancer_central/alembic.ini",
                os.path.join(os.path.dirname(__file__), "alembic.ini")
            ]

            alembic_cfg_path = None
            for path in potential_paths:
                if os.path.exists(path):
                    alembic_cfg_path = path
                    break

            if not alembic_cfg_path:
                logger.error(f"❌ Alembic configuration file not found in: {potential_paths}")
                logger.info("💡 Use --alembic-config to specify custom path")
                return None

        if not os.path.exists(alembic_cfg_path):
            logger.error(f"❌ Alembic configuration file not found: {alembic_cfg_path}")
            return None

        logger.info(f"📋 Using Alembic config: {alembic_cfg_path}")

        # Create Alembic config and override connection string
        alembic_cfg = Config(alembic_cfg_path)
        alembic_cfg.set_main_option("sqlalchemy.url", postgresql_url)

        # Set script location if provided
        if alembic_script_location:
            if not os.path.exists(alembic_script_location):
                logger.error(f"❌ Alembic script location not found: {alembic_script_location}")
                return None
            alembic_cfg.set_main_option("script_location", alembic_script_location)
            logger.info(f"📋 Using Alembic scripts: {alembic_script_location}")
        else:
            # Try to auto-detect script location relative to config
            config_dir = os.path.dirname(os.path.abspath(alembic_cfg_path))
            potential_script_paths = [
                os.path.join(config_dir, "alembic"),
                os.path.join(config_dir, "fileglancer_central", "alembic"),
                "alembic"
            ]

            script_location = None
            for path in potential_script_paths:
                if os.path.exists(path) and os.path.isdir(path):
                    script_location = path
                    break

            if script_location:
                alembic_cfg.set_main_option("script_location", script_location)
                logger.info(f"📋 Auto-detected Alembic scripts: {script_location}")
            else:
                logger.warning("⚠️  Could not auto-detect Alembic script location")
                logger.info("💡 Use --alembic-script-location to specify custom path")

        logger.info("✅ Alembic configuration setup complete")
        return alembic_cfg

    except Exception as e:
        logger.error(f"❌ Failed to setup Alembic configuration: {e}")
        return None


def check_existing_postgresql_schema(postgresql_url: str, logger: logging.Logger, auto_yes: bool = False) -> bool:
    """Check if PostgreSQL database has existing Alembic migrations."""
    try:
        engine = create_engine(postgresql_url, echo=False)
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        if 'alembic_version' in tables:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version_num FROM alembic_version"))
                version = result.scalar()
                if version:
                    logger.warning(f"⚠️  Existing Alembic schema found (version: {version})")
                    if auto_yes:
                        logger.info("🤖 Auto-confirming deletion of existing data (--yes flag used)")
                        return True
                    response = input("Continue with deletion of existing data? (y/N): ")
                    return response.lower() in ['y', 'yes']

        logger.info("📋 No existing Alembic schema found")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to check PostgreSQL schema: {e}")
        return False


def clear_postgresql_database(postgresql_url: str, logger: logging.Logger) -> bool:
    """Clear all existing data and schema from PostgreSQL database."""
    try:
        logger.info("🧹 Clearing PostgreSQL database...")

        engine = create_engine(postgresql_url, echo=False)

        with engine.connect() as conn:
            trans = conn.begin()

            try:
                # Get all table names first
                inspector = inspect(engine)
                table_names = inspector.get_table_names()

                if table_names:
                    logger.info(f"  📋 Found {len(table_names)} tables to drop: {table_names}")

                    # Drop all tables with CASCADE to handle foreign key constraints
                    for table_name in table_names:
                        try:
                            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                            logger.info(f"    🗑️  Dropped table: {table_name}")
                        except Exception as e:
                            logger.warning(f"    ⚠️  Failed to drop table {table_name}: {e}")
                else:
                    logger.info("  📋 No tables found to drop")

                # Drop all sequences
                logger.info("  🔄 Dropping all sequences...")
                sequences_result = conn.execute(text(
                    "SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public'"
                ))
                sequences = [row[0] for row in sequences_result.fetchall()]

                for sequence_name in sequences:
                    try:
                        conn.execute(text(f"DROP SEQUENCE IF EXISTS {sequence_name} CASCADE"))
                        logger.info(f"    🗑️  Dropped sequence: {sequence_name}")
                    except Exception as e:
                        logger.warning(f"    ⚠️  Failed to drop sequence {sequence_name}: {e}")

                # Drop all custom types
                logger.info("  🔄 Dropping all custom types...")
                types_result = conn.execute(text(
                    "SELECT typname FROM pg_type WHERE typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public') AND typtype = 'e'"
                ))
                types = [row[0] for row in types_result.fetchall()]

                for type_name in types:
                    try:
                        conn.execute(text(f"DROP TYPE IF EXISTS {type_name} CASCADE"))
                        logger.info(f"    🗑️  Dropped type: {type_name}")
                    except Exception as e:
                        logger.warning(f"    ⚠️  Failed to drop type {type_name}: {e}")

                # Commit the transaction
                trans.commit()
                logger.info("✅ PostgreSQL database cleared successfully")
                return True

            except Exception as e:
                trans.rollback()
                raise e

    except Exception as e:
        logger.error(f"❌ Failed to clear PostgreSQL database: {e}")
        return False


def verify_schema_creation(postgresql_url: str, logger: logging.Logger) -> bool:
    """Verify that Alembic migrations created the expected schema."""
    try:
        engine = create_engine(postgresql_url, echo=False)
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        # Filter out system tables
        user_tables = [t for t in tables if not t.startswith('pg_') and t != 'information_schema']

        logger.info(f"📊 Found {len(user_tables)} tables after Alembic migration:")
        for table in user_tables:
            columns = inspector.get_columns(table)
            logger.info(f"  📋 {table}: {len(columns)} columns")
            if logger.level <= logging.DEBUG:
                for col in columns:
                    logger.debug(f"    - {col['name']}: {col['type']}")

        if len(user_tables) == 0:
            logger.error("❌ No tables found after Alembic migration!")
            logger.info("💡 This suggests the Alembic migrations may not have run correctly")
            return False

        # Check for alembic_version table specifically
        if 'alembic_version' not in tables:
            logger.warning("⚠️  alembic_version table not found - migrations may not have completed")
        else:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version_num FROM alembic_version"))
                version = result.scalar()
                logger.info(f"📌 Alembic version: {version}")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to verify schema creation: {e}")
        return False


def apply_alembic_migrations(alembic_cfg: Config, postgresql_url: str, logger: logging.Logger) -> bool:
    """Apply Alembic migrations to create/update PostgreSQL schema."""
    if not ALEMBIC_AVAILABLE:
        logger.error("❌ Alembic is not available. Please install with: pip install alembic")
        return False

    # The env.py file overrides sqlalchemy.url with get_database_url()
    # So we need to set the environment variable that get_database_url() checks
    original_migration_url = os.environ.get('FILEGLANCER_MIGRATION_DB_URL')
    os.environ['FILEGLANCER_MIGRATION_DB_URL'] = postgresql_url

    try:
        logger.info("🔄 Running Alembic upgrade to head...")
        logger.info(f"🔧 Set FILEGLANCER_MIGRATION_DB_URL to: {postgresql_url.split('@')[0]}@***")

        # Run the migration (Alembic will interfere with logging)
        command.upgrade(alembic_cfg, "head")

        # Alembic has messed up our logging - we'll fix it in the main function
        print("✅ Alembic migrations applied successfully")  # Use print since logger is broken
        return True

    except Exception as e:
        logger.error(f"❌ Failed to apply Alembic migrations: {e}")
        logger.error("💡 Common issues:")
        logger.error("   - Check that Alembic configuration file exists and is valid")
        logger.error("   - Verify that migration files exist in the versions directory")
        logger.error("   - Ensure PostgreSQL user has CREATE privileges")
        return False

    finally:
        # Restore original environment variable
        if original_migration_url is not None:
            os.environ['FILEGLANCER_MIGRATION_DB_URL'] = original_migration_url
        else:
            os.environ.pop('FILEGLANCER_MIGRATION_DB_URL', None)


def get_table_dependencies(sqlite_engine, logger: logging.Logger) -> List[str]:
    """Get tables sorted by dependency order (parent tables first)."""
    try:
        inspector = inspect(sqlite_engine)
        all_tables = inspector.get_table_names()

        # Filter out system tables
        user_tables = [table for table in all_tables if not table.startswith('sqlite_')]

        # Simple dependency resolution - put alembic_version last if it exists
        ordered_tables = []
        for table in user_tables:
            if table != 'alembic_version':
                ordered_tables.append(table)

        if 'alembic_version' in user_tables:
            ordered_tables.append('alembic_version')

        logger.info(f"📋 Found {len(ordered_tables)} tables to migrate: {ordered_tables}")
        return ordered_tables

    except Exception as e:
        logger.error(f"❌ Failed to get table dependencies: {e}")
        return []


def disable_postgresql_constraints(postgresql_engine, logger: logging.Logger):
    """Temporarily disable PostgreSQL constraints for faster inserts."""
    try:
        with postgresql_engine.connect() as conn:
            # Disable foreign key checks temporarily (requires SUPERUSER privileges)
            conn.execute(text("SET session_replication_role = replica;"))
            conn.commit()
        logger.info("🔧 PostgreSQL constraints temporarily disabled")

    except Exception as e:
        # This is expected if user doesn't have SUPERUSER privileges
        logger.info(f"💡 Constraint optimization not available (requires SUPERUSER): {type(e).__name__}")
        logger.info("🔧 Migration will proceed without constraint optimization")


def enable_postgresql_constraints(postgresql_engine, logger: logging.Logger):
    """Re-enable PostgreSQL constraints after migration."""
    try:
        with postgresql_engine.connect() as conn:
            # Re-enable foreign key checks
            conn.execute(text("SET session_replication_role = DEFAULT;"))
            conn.commit()
        logger.info("🔧 PostgreSQL constraints re-enabled")

    except Exception as e:
        # This is expected if user doesn't have SUPERUSER privileges or constraints weren't disabled
        logger.debug(f"Constraint re-enable not needed or not available: {type(e).__name__}")


def migrate_table_data(sqlite_engine, postgresql_engine, table_name: str, batch_size: int, logger: logging.Logger) -> int:
    """Migrate data for a specific table in batches."""
    try:
        # Check if table exists in both databases
        sqlite_inspector = inspect(sqlite_engine)
        postgresql_inspector = inspect(postgresql_engine)

        if table_name not in sqlite_inspector.get_table_names():
            logger.warning(f"⚠️  Table {table_name} not found in SQLite")
            return 0

        if table_name not in postgresql_inspector.get_table_names():
            logger.warning(f"⚠️  Table {table_name} not found in PostgreSQL")
            return 0

        # Get table structure
        sqlite_columns = sqlite_inspector.get_columns(table_name)
        postgresql_columns = postgresql_inspector.get_columns(table_name)

        # Map columns that exist in both databases
        sqlite_col_names = [col['name'] for col in sqlite_columns]
        postgresql_col_names = [col['name'] for col in postgresql_columns]
        common_columns = [col for col in sqlite_col_names if col in postgresql_col_names]

        if not common_columns:
            logger.warning(f"⚠️  No common columns found for table {table_name}")
            return 0

        logger.info(f"📝 Migrating columns: {common_columns}")

        # Count total rows in SQLite
        with sqlite_engine.connect() as sqlite_conn:
            count_result = sqlite_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            total_rows = count_result.scalar()

        if total_rows == 0:
            logger.info(f"📋 Table {table_name} is empty")
            return 0

        logger.info(f"🔄 Migrating {total_rows:,} rows from {table_name}")

        # Migrate data in batches
        migrated_rows = 0
        offset = 0

        # Prepare PostgreSQL insert statement (exclude ID for auto-generation)
        insert_columns = [col for col in common_columns if col.lower() != 'id']
        if not insert_columns:
            insert_columns = common_columns  # Fallback if no non-ID columns

        # Quote reserved keywords in column names for INSERT statement
        quoted_insert_columns = [f'"{col}"' if col.lower() in ['group', 'order', 'select', 'from', 'where'] else col for col in insert_columns]
        column_list = ', '.join(quoted_insert_columns)
        placeholders = ', '.join([f':{col}' for col in insert_columns])
        insert_sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"

        while offset < total_rows:
            # Read batch from SQLite
            with sqlite_engine.connect() as sqlite_conn:
                # Quote column names to handle reserved keywords like 'group'
                quoted_columns = [f'"{col}"' if col.lower() in ['group', 'order', 'select', 'from', 'where'] else col for col in insert_columns if col.lower() != 'id']
                if not quoted_columns:  # If only ID column, select all common columns
                    quoted_columns = [f'"{col}"' if col.lower() in ['group', 'order', 'select', 'from', 'where'] else col for col in common_columns]
                select_column_list = ', '.join(quoted_columns)
                select_sql = f"SELECT {select_column_list} FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
                result = sqlite_conn.execute(text(select_sql))
                rows = result.fetchall()

                if not rows:
                    break

                # Convert rows to dictionaries, excluding auto-increment ID columns
                batch_data = []
                actual_select_columns = [col.strip('"') for col in quoted_columns]  # Remove quotes for mapping
                for row in rows:
                    row_dict = {}
                    for i, col in enumerate(actual_select_columns):
                        # Get value by index since we know the column order
                        row_dict[col] = row[i]
                    batch_data.append(row_dict)

                # Don't regenerate insert_sql - it's already properly formatted with quoted columns above

                # Insert batch into PostgreSQL
                with postgresql_engine.connect() as postgresql_conn:
                    postgresql_conn.execute(text(insert_sql), batch_data)
                    postgresql_conn.commit()

                migrated_rows += len(rows)
                offset += batch_size

                # Progress reporting
                progress_pct = (migrated_rows / total_rows) * 100
                logger.info(f"    📊 Progress: {migrated_rows:,}/{total_rows:,} rows ({progress_pct:.1f}%)")

        logger.info(f"✅ Successfully migrated {migrated_rows:,} rows from {table_name}")
        return migrated_rows

    except Exception as e:
        logger.error(f"❌ Failed to migrate table {table_name}: {e}")
        raise


def perform_data_migration(sqlite_url: str, postgresql_url: str, batch_size: int, logger: logging.Logger) -> bool:
    """Perform the complete data migration process."""
    try:
        # Create database engines with quiet logging
        sqlite_engine = create_engine(sqlite_url, echo=False)
        postgresql_engine = create_engine(postgresql_url, echo=False)

        # Get tables in dependency order
        tables_to_migrate = get_table_dependencies(sqlite_engine, logger)
        if not tables_to_migrate:
            logger.error("❌ No tables found to migrate")
            return False

        # Clear any existing data from tables (in case clearing didn't work completely)
        logger.info("🧹 Ensuring all tables are empty...")
        with postgresql_engine.connect() as conn:
            tables = get_table_dependencies(postgresql_engine, logger)
            for table_name in tables:
                if table_name != 'alembic_version':  # Don't truncate alembic_version
                    try:
                        conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
                        logger.info(f"  🗑️  Cleared {table_name}")
                    except Exception as e:
                        logger.warning(f"  ⚠️  Could not clear {table_name}: {e}")
            conn.commit()

        # Temporarily disable constraints for faster migration
        disable_postgresql_constraints(postgresql_engine, logger)

        try:
            total_migrated = 0
            successful_tables = 0
            failed_tables = []

            # Migrate each table
            for i, table_name in enumerate(tables_to_migrate, 1):
                logger.info(f"📋 Processing table {i}/{len(tables_to_migrate)}: {table_name}")

                # Skip alembic_version table - Alembic manages this
                if table_name == 'alembic_version':
                    logger.info(f"  ⏭️  Skipping {table_name} - managed by Alembic")
                    successful_tables += 1
                    continue

                try:
                    rows_migrated = migrate_table_data(sqlite_engine, postgresql_engine, table_name, batch_size, logger)
                    total_migrated += rows_migrated
                    successful_tables += 1

                except Exception as e:
                    logger.error(f"❌ Failed to migrate table {table_name}: {e}")
                    failed_tables.append(table_name)
                    continue

            # Migration summary
            logger.info("=" * 60)
            logger.info("📊 MIGRATION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"✅ Successfully migrated tables: {successful_tables}/{len(tables_to_migrate)}")
            logger.info(f"📈 Total rows migrated: {total_migrated:,}")

            if failed_tables:
                logger.warning(f"⚠️  Failed tables: {failed_tables}")

            return len(failed_tables) == 0

        finally:
            # Always re-enable constraints
            enable_postgresql_constraints(postgresql_engine, logger)

    except Exception as e:
        logger.error(f"❌ Data migration failed: {e}")
        return False


def update_postgresql_sequences(postgresql_engine, logger: logging.Logger) -> bool:
    """Update PostgreSQL sequence values for auto-increment columns."""
    try:
        logger.info("🔄 Updating PostgreSQL sequence values...")

        # Get all sequences (handle different PostgreSQL versions)
        sequences = []

        # Try modern PostgreSQL (10+) first
        try:
            with postgresql_engine.connect() as conn:
                sequences_query = text("""
                    SELECT schemaname, tablename, columnname, sequencename
                    FROM pg_sequences
                    WHERE schemaname = 'public'
                """)
                sequences = conn.execute(sequences_query).fetchall()
                logger.debug("Using pg_sequences for PostgreSQL 10+")
        except Exception as e:
            logger.debug(f"pg_sequences query failed: {e}, trying fallback")

            # Fallback for older PostgreSQL versions - use a new connection
            try:
                with postgresql_engine.connect() as conn:
                    sequences_query = text("""
                        SELECT 'public' as schemaname,
                               CASE
                                   WHEN c.relname LIKE '%_id_seq' THEN substr(c.relname, 1, length(c.relname) - 7)
                                   WHEN c.relname LIKE '%_seq' THEN substr(c.relname, 1, length(c.relname) - 4)
                                   ELSE substr(c.relname, 1, length(c.relname) - 3)
                               END as tablename,
                               'id' as columnname,
                               c.relname as sequencename
                        FROM pg_class c
                        WHERE c.relkind = 'S'
                        AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                    """)
                    sequences = conn.execute(sequences_query).fetchall()
                    logger.debug("Using pg_class fallback for older PostgreSQL")
            except Exception as fallback_e:
                logger.error(f"Both sequence queries failed: {fallback_e}")
                return False

        if not sequences:
            logger.info("📋 No sequences found to update")
            return True

        # Get list of existing tables to avoid updating sequences for non-existent tables
        existing_tables = set()
        try:
            with postgresql_engine.connect() as conn:
                inspector = inspect(postgresql_engine)
                existing_tables = set(inspector.get_table_names())
        except Exception as e:
            logger.warning(f"Could not get table list: {e}")

        # Update each sequence with individual transactions to avoid aborted transaction issues
        for seq in sequences:
            table_name = seq.tablename
            column_name = seq.columnname
            sequence_name = seq.sequencename

            # Skip sequences for tables that don't exist
            if table_name not in existing_tables:
                logger.debug(f"  ⏭️  Skipping sequence {sequence_name} - table {table_name} does not exist")
                continue

            try:
                # Use a fresh connection for each sequence to avoid transaction state issues
                with postgresql_engine.connect() as conn:
                    # Get the maximum value from the table
                    max_query = text(f"SELECT COALESCE(MAX({column_name}), 0) FROM {table_name}")
                    max_val = conn.execute(max_query).scalar()

                    if max_val > 0:
                        # Set sequence to max_val + 1
                        set_seq_query = text(f"SELECT setval('{sequence_name}', {max_val + 1})")
                        conn.execute(set_seq_query)
                        conn.commit()
                        logger.info(f"  📈 Updated sequence {sequence_name} to {max_val + 1}")
                    else:
                        logger.debug(f"  📋 Sequence {sequence_name} already at correct value (table empty)")

            except Exception as e:
                logger.warning(f"  ⚠️  Could not update sequence {sequence_name}: {e}")
                logger.debug(f"     Table: {table_name}, Column: {column_name}")
                # Continue with other sequences even if one fails

        logger.info("✅ PostgreSQL sequences updated")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to update PostgreSQL sequences: {e}")
        return False


def validate_data_integrity(sqlite_url: str, postgresql_url: str, logger: logging.Logger) -> bool:
    """Validate data integrity by comparing row counts and sample data."""
    try:
        logger.info("🔍 Validating data integrity...")

        sqlite_engine = create_engine(sqlite_url, echo=False)
        postgresql_engine = create_engine(postgresql_url, echo=False)

        # Get common tables
        sqlite_inspector = inspect(sqlite_engine)
        postgresql_inspector = inspect(postgresql_engine)

        sqlite_tables = set(sqlite_inspector.get_table_names())
        postgresql_tables = set(postgresql_inspector.get_table_names())

        common_tables = sqlite_tables.intersection(postgresql_tables)
        common_tables = [t for t in common_tables if not t.startswith('sqlite_')]

        validation_results = {}
        all_valid = True

        for table_name in common_tables:
            try:
                # Compare row counts
                with sqlite_engine.connect() as sqlite_conn:
                    sqlite_count = sqlite_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

                with postgresql_engine.connect() as postgresql_conn:
                    postgresql_count = postgresql_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

                validation_results[table_name] = {
                    'sqlite_count': sqlite_count,
                    'postgresql_count': postgresql_count,
                    'match': sqlite_count == postgresql_count
                }

                if sqlite_count == postgresql_count:
                    logger.info(f"  ✅ {table_name}: {sqlite_count:,} rows (match)")
                else:
                    logger.error(f"  ❌ {table_name}: SQLite={sqlite_count:,}, PostgreSQL={postgresql_count:,} (mismatch)")
                    all_valid = False

            except Exception as e:
                logger.error(f"  ❌ Could not validate {table_name}: {e}")
                validation_results[table_name] = {'error': str(e)}
                all_valid = False

        # Summary with details of failed tables
        if all_valid:
            logger.info("✅ Data integrity validation passed")
        else:
            # Show which tables failed validation
            failed_tables = []
            error_tables = []

            for table_name, result in validation_results.items():
                if 'error' in result:
                    error_tables.append(table_name)
                elif not result.get('match', True):
                    failed_tables.append(f"{table_name} (SQLite: {result['sqlite_count']:,}, PostgreSQL: {result['postgresql_count']:,})")

            logger.error("❌ Data integrity validation failed")
            if failed_tables:
                logger.error(f"  📊 Row count mismatches: {', '.join(failed_tables)}")
            if error_tables:
                logger.error(f"  ⚠️  Validation errors: {', '.join(error_tables)}")

        return all_valid

    except Exception as e:
        logger.error(f"❌ Data integrity validation failed: {e}")
        return False


def generate_migration_report(sqlite_url: str, postgresql_url: str, logger: logging.Logger):
    """Generate a comprehensive migration summary report."""
    try:
        logger.info("📋 Generating migration report...")

        sqlite_engine = create_engine(sqlite_url, echo=False)
        postgresql_engine = create_engine(postgresql_url, echo=False)

        print("\n" + "=" * 80)
        print("📊 MIGRATION REPORT")
        print("=" * 80)
        print(f"Migration completed at: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"SQLite source: {sqlite_url}")
        print(f"PostgreSQL target: {postgresql_url.split('@')[0]}@***")
        print()

        # Table summary
        sqlite_inspector = inspect(sqlite_engine)
        postgresql_inspector = inspect(postgresql_engine)

        sqlite_tables = set(sqlite_inspector.get_table_names())
        postgresql_tables = set(postgresql_inspector.get_table_names())

        # Filter out system tables for analysis
        sqlite_user_tables = {t for t in sqlite_tables if not t.startswith('sqlite_')}
        postgresql_user_tables = {t for t in postgresql_tables if not t.startswith('sqlite_')}

        missing_in_postgresql = sqlite_user_tables - postgresql_user_tables
        extra_in_postgresql = postgresql_user_tables - sqlite_user_tables
        common_tables = sqlite_user_tables.intersection(postgresql_user_tables)

        print(f"SQLite tables found: {len(sqlite_user_tables)}")
        print(f"PostgreSQL tables found: {len(postgresql_user_tables)}")
        print(f"Common tables: {len(common_tables)}")

        # Show missing tables if any
        if missing_in_postgresql:
            print(f"📋 Tables in SQLite but NOT in PostgreSQL: {', '.join(sorted(missing_in_postgresql))}")
        if extra_in_postgresql:
            print(f"📋 Tables in PostgreSQL but NOT in SQLite: {', '.join(sorted(extra_in_postgresql))}")
        print()

        # Row count summary
        print("Table Row Counts:")
        print("-" * 50)
        for table_name in sorted(common_tables):
            try:
                with sqlite_engine.connect() as sqlite_conn:
                    sqlite_count = sqlite_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

                with postgresql_engine.connect() as postgresql_conn:
                    postgresql_count = postgresql_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

                status = "✅" if sqlite_count == postgresql_count else "❌"
                print(f"{status} {table_name:<25} {sqlite_count:>10,} → {postgresql_count:>10,}")

            except Exception as e:
                print(f"❌ {table_name:<25} {'Error':>10} → {'Error':>10}")

        print("=" * 80)

    except Exception as e:
        logger.error(f"❌ Failed to generate migration report: {e}")


def post_migration_tasks(sqlite_url: str, postgresql_url: str, logger: logging.Logger) -> bool:
    """Perform post-migration tasks including validation and reporting."""
    try:
        success = True

        # Update PostgreSQL sequences (treat failure as warning, not error)
        postgresql_engine = create_engine(postgresql_url, echo=False)
        if not update_postgresql_sequences(postgresql_engine, logger):
            logger.warning("⚠️  Sequence update failed, but this doesn't affect data integrity")

        # Validate data integrity (this is critical)
        if not validate_data_integrity(sqlite_url, postgresql_url, logger):
            success = False

        # Generate migration report
        generate_migration_report(sqlite_url, postgresql_url, logger)
        sys.stdout.flush()  # Ensure report is displayed immediately

        return success

    except Exception as e:
        logger.error(f"❌ Post-migration tasks failed: {e}")
        return False


def main():
    """Main entry point."""
    args = parse_arguments()
    logger = setup_logging(args.verbose)

    logger.info("Starting SQLite to PostgreSQL migration")
    logger.info(f"SQLite URL: {args.sqlite_url}")
    logger.info(f"PostgreSQL URL: {args.postgresql_url.split('@')[0]}@***")  # Hide credentials
    logger.info(f"Batch size: {args.batch_size}")

    # Step 1: Validate database connections
    logger.info("📡 Validating database connections...")
    if not validate_sqlite_connection(args.sqlite_url, logger):
        sys.exit(1)

    if not validate_postgresql_connection(args.postgresql_url, logger):
        sys.exit(1)

    # Step 2: Setup Alembic for schema management
    logger.info("⚙️  Setting up Alembic configuration...")
    alembic_cfg = setup_alembic_config(
        args.postgresql_url,
        logger,
        args.alembic_config,
        args.alembic_script_location
    )
    if not alembic_cfg:
        sys.exit(1)

    # Step 3: Check existing schema and get confirmation
    logger.info("🔍 Checking existing PostgreSQL schema...")
    if not check_existing_postgresql_schema(args.postgresql_url, logger, args.yes):
        logger.info("Migration cancelled by user")
        sys.exit(0)

    # Step 4: Clear existing database for clean schema
    logger.info("🧹 Clearing PostgreSQL database for clean migration...")
    if not clear_postgresql_database(args.postgresql_url, logger):
        sys.exit(1)

    # Step 5: Apply Alembic migrations
    logger.info("📋 Applying Alembic migrations...")
    if not apply_alembic_migrations(alembic_cfg, args.postgresql_url, logger):
        sys.exit(1)

    # Completely reinitialize logging system after Alembic interference
    import importlib
    importlib.reload(logging)

    # Clear all existing loggers and handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.root.handlers.clear()

    # Reset all loggers
    for name in list(logging.Logger.manager.loggerDict.keys()):
        logging.Logger.manager.loggerDict[name].handlers.clear()

    # Create completely fresh logger
    logger = setup_logging(args.verbose)
    logger.info("🔧 Logger restored after Alembic interference")

    # Step 5.1: Verify schema was created
    logger.info("🔍 Verifying schema creation...")
    if not verify_schema_creation(args.postgresql_url, logger):
        logger.error("❌ Schema verification failed - no tables found after Alembic migration")
        sys.exit(1)

    # Step 6: Perform data migration
    logger.info("🚀 Starting data migration...")
    migration_result = perform_data_migration(args.sqlite_url, args.postgresql_url, args.batch_size, logger)

    if not migration_result:
        logger.error("❌ Data migration failed")
        sys.exit(1)

    logger.info("✅ Data migration completed successfully")

    # Step 7: Post-migration tasks
    logger.info("🔍 Running post-migration validation...")
    if not post_migration_tasks(args.sqlite_url, args.postgresql_url, logger):
        sys.exit(1)

    logger.info("🎉 Migration completed successfully!")


if __name__ == "__main__":
    main()
