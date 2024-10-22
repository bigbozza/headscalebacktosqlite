import psycopg2
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
import sys

# Configure Logging
logging.basicConfig(
    level=logging.DEBUG,   # Set to DEBUG for more verbose output
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("migration.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
POSTGRES_CONFIG = {
    'host': 'enter.address.here',
    'port': '5432',
    'dbname': 'headscale',
    'user': 'headscale',
    'password': 'PasswordHere'
}

SQLITE_DB_PATH = './db.sqlite'

# Option to disable foreign key constraints
DISABLE_FOREIGN_KEYS = True  # Set to True to disable during migration

# Connect to PostgreSQL
def connect_postgres():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG['host'],
            port=POSTGRES_CONFIG['port'],
            dbname=POSTGRES_CONFIG['dbname'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password']
        )
        logger.info("Successfully connected to PostgreSQL.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        sys.exit(1)

# Connect to SQLite
def connect_sqlite(db_path):
    try:
        conn = sqlite3.connect(db_path)
        if DISABLE_FOREIGN_KEYS:
            conn.execute("PRAGMA foreign_keys = OFF;")
            logger.info("Foreign key constraints DISABLED in SQLite.")
        else:
            conn.execute("PRAGMA foreign_keys = ON;")
            logger.info("Foreign key constraints ENABLED in SQLite.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to SQLite: {e}")
        sys.exit(1)

# Conversion Functions
def convert_boolean(value):
    if value is None:
        return None
    return 1 if value else 0

def convert_timestamp(value):
    if value is None:
        return None
    return value.isoformat()

def convert_bytea(value):
    if value is None:
        return None
    return value  # SQLite handles blobs as bytes

def convert_json(value):
    if value is None:
        return None
    return str(value)  # Store JSON as TEXT

def convert_array(value):
    if value is None:
        return None
    if isinstance(value, list):
        return ','.join(map(str, value))
    return str(value)

# Data Type Mapping from PostgreSQL to SQLite
DATA_TYPE_MAPPING = {
    'bigint': 'INTEGER',
    'integer': 'INTEGER',
    'smallint': 'INTEGER',
    'serial': 'INTEGER',
    'text': 'TEXT',
    'varchar': 'TEXT',
    'character varying': 'TEXT',
    'bytea': 'BLOB',
    'boolean': 'NUMERIC',
    'timestamp with time zone': 'TEXT',
    'timestamp without time zone': 'TEXT',
    'date': 'TEXT',
    'time with time zone': 'TEXT',
    'time without time zone': 'TEXT',
    'numeric': 'REAL',
    'real': 'REAL',
    'double precision': 'REAL',
    'json': 'TEXT',
    'jsonb': 'TEXT'
    # Add more mappings as needed
}

# List of tables to migrate (Reordered to satisfy foreign key constraints)
TABLES = [
    'users',
    'pre_auth_keys',
    'pre_auth_key_acl_tags',
    'api_keys',
    'nodes',
    'routes',
    'policies'
    # Removed 'migrations' to avoid UNIQUE constraint violations
]

# Mapping of tables to their special columns and conversion functions
SPECIAL_CONVERSIONS = {
    'api_keys': {
        'hash': convert_bytea,
        'created_at': convert_timestamp,
        'expiration': convert_timestamp,
        'last_seen': convert_timestamp
    },
    'nodes': {
        'last_seen': convert_timestamp,
        'expiry': convert_timestamp,
        'created_at': convert_timestamp,
        'updated_at': convert_timestamp,
        'deleted_at': convert_timestamp,
        'host_info': convert_json,
        'endpoints': convert_array
    },
    'pre_auth_keys': {
        'reusable': convert_boolean,
        'ephemeral': convert_boolean,
        'used': convert_boolean,
        'created_at': convert_timestamp,
        'expiration': convert_timestamp
    },
    'routes': {
        'created_at': convert_timestamp,
        'updated_at': convert_timestamp,
        'deleted_at': convert_timestamp
    },
    'users': {
        'created_at': convert_timestamp,
        'updated_at': convert_timestamp,
        'deleted_at': convert_timestamp
    }
}

# Function to get PostgreSQL columns for a table
def get_postgres_columns(pg_conn, table_name):
    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s;
    """
    with pg_conn.cursor() as cursor:
        cursor.execute(query, (table_name,))
        columns = cursor.fetchall()
    return {col[0]: col[1] for col in columns}

# Function to get SQLite columns for a table
def get_sqlite_columns(sqlite_conn, table_name):
    try:
        cursor = sqlite_conn.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        return {col[1]: col[2] for col in columns}  # {column_name: data_type}
    except sqlite3.OperationalError:
        logger.warning(f"SQLite table '{table_name}' does not exist.")
        return {}

# Function to add missing columns to SQLite table
def add_missing_columns(sqlite_conn, table_name, missing_columns, pg_columns):
    for col in missing_columns:
        pg_data_type = pg_columns[col]
        sqlite_data_type = DATA_TYPE_MAPPING.get(pg_data_type, 'TEXT')  # Default to TEXT
        try:
            alter_stmt = f"ALTER TABLE {table_name} ADD COLUMN {col} {sqlite_data_type};"
            sqlite_conn.execute(alter_stmt)
            logger.info(f"Added missing column '{col}' of type '{sqlite_data_type}' to SQLite table '{table_name}'.")
        except Exception as e:
            logger.error(f"Failed to add column '{col}' to table '{table_name}': {e}")

# Function to handle column mapping
def handle_column_mapping(df, table_name, sqlite_conn):
    if table_name == 'routes':
        if 'machine_id' in df.columns:
            logger.info(f"Mapping 'machine_id' to 'node_id' in table '{table_name}'.")
            df['node_id'] = df.apply(
                lambda row: row['machine_id'] if (row['node_id'] == 0 or pd.isnull(row['node_id'])) and (row['machine_id'] > 0) else row['node_id'],
                axis=1
            )
            df = df.drop(columns=['machine_id'])
            logger.debug(f"Mapped 'machine_id' to 'node_id' for table '{table_name}'.")
    return df

# Function to recreate the `api_keys` table with the correct definition
def recreate_api_keys_table(sqlite_conn):
    try:
        # Drop the existing api_keys table if it exists
        sqlite_conn.execute("DROP TABLE IF EXISTS api_keys;")
        logger.info("Dropped existing 'api_keys' table in SQLite.")

        # Recreate the api_keys table with the correct definition
        create_table_query = """
        CREATE TABLE "api_keys" (
            "id" integer PRIMARY KEY,
            "prefix" text UNIQUE,
            "hash" blob,
            "created_at" datetime,
            "expiration" datetime,
            "last_seen" datetime
        );
        """
        sqlite_conn.execute(create_table_query)
        logger.info("Recreated 'api_keys' table with the correct definition.")

        # Create the unique index for prefix
        sqlite_conn.execute("CREATE UNIQUE INDEX idx_api_keys_prefix ON api_keys(prefix);")
        logger.info("Created unique index 'idx_api_keys_prefix' on 'api_keys' table.")

    except Exception as e:
        logger.error(f"Failed to recreate 'api_keys' table: {e}")

# Function to migrate a single table
def migrate_table(pg_conn, sqlite_conn, table_name):
    logger.info(f"Starting migration for table: {table_name}")
    try:
        # Get PostgreSQL columns
        pg_columns = get_postgres_columns(pg_conn, table_name)

        # Get SQLite columns
        sqlite_columns = get_sqlite_columns(sqlite_conn, table_name)

        # Determine missing columns
        missing_columns = [col for col in pg_columns if col not in sqlite_columns]
        if missing_columns:
            logger.info(f"Missing columns in SQLite table '{table_name}': {missing_columns}")
            add_missing_columns(sqlite_conn, table_name, missing_columns, pg_columns)
        else:
            logger.info(f"No missing columns in SQLite table '{table_name}'.")

        # Fetch all data from PostgreSQL
        with pg_conn.cursor() as pg_cursor:
            pg_cursor.execute(f'SELECT * FROM {table_name};')
            rows = pg_cursor.fetchall()
            columns = [desc[0] for desc in pg_cursor.description]
            initial_row_count = len(rows)
            logger.info(f"Fetched {initial_row_count} rows from PostgreSQL table '{table_name}'.")

        if initial_row_count == 0:
            logger.warning(f"No data found in PostgreSQL table '{table_name}'. Skipping migration.")
            return

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=columns)
        logger.info(f"Initial DataFrame row count for '{table_name}': {len(df)}")

        # Handle column mapping
        df = handle_column_mapping(df, table_name, sqlite_conn)

        # Apply special conversions if any
        if table_name in SPECIAL_CONVERSIONS:
            for col, func in SPECIAL_CONVERSIONS[table_name].items():
                if col in df.columns:
                    logger.debug(f"Applying conversion for column '{col}' in table '{table_name}'.")
                    df[col] = df[col].apply(func)

        # Handle 'auth_key_id' in 'nodes' table
        if table_name == 'nodes' and 'auth_key_id' in df.columns:
            # Log distribution of auth_key_id values
            logger.info(f"Distribution of auth_key_id values: {df['auth_key_id'].value_counts(dropna=False).to_dict()}")

            # Replace 'auth_key_id' values of 0 with NULL
            df['auth_key_id'] = df['auth_key_id'].replace(0, pd.NA)

            # Fetch all valid 'auth_key_id's from 'pre_auth_keys'
            with pg_conn.cursor() as cursor:
                cursor.execute("SELECT id FROM pre_auth_keys;")
                valid_auth_key_ids = set([row[0] for row in cursor.fetchall()])

            # Identify invalid 'auth_key_id's in df
            invalid_mask = df['auth_key_id'].notna() & ~df['auth_key_id'].isin(valid_auth_key_ids)

            if invalid_mask.any():
                invalid_auth_keys = df.loc[invalid_mask, 'auth_key_id'].unique()
                logger.error(f"Foreign key constraint violation in table 'nodes' for 'auth_key_id'. Invalid keys: {invalid_auth_keys}")

                # Set invalid 'auth_key_id's to NULL
                df.loc[invalid_mask, 'auth_key_id'] = pd.NA

            logger.info(f"Final DataFrame row count for '{table_name}' after processing: {len(df)}")

        # Insert into SQLite
        try:
            df.to_sql(table_name, sqlite_conn, if_exists='append', index=False)

            # Verify the number of rows inserted
            cursor = sqlite_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            actual_count = cursor.fetchone()[0]
            logger.info(f"Actually inserted {actual_count} rows into SQLite table '{table_name}'.")

            if actual_count != len(df):
                logger.warning(f"Discrepancy in row count for table '{table_name}'. Expected: {len(df)}, Actual: {actual_count}")

        except sqlite3.IntegrityError as e:
            logger.error(f"IntegrityError migrating table '{table_name}': {e}")
        except Exception as e:
            logger.error(f"Unexpected error migrating table '{table_name}': {e}")

    except Exception as e:
        logger.error(f"Error migrating table '{table_name}': {e}")

def main():
    logger.info("Starting PostgreSQL to SQLite migration process.")

    # Connect to PostgreSQL
    pg_conn = connect_postgres()

    # Connect to SQLite
    sqlite_conn = connect_sqlite(SQLITE_DB_PATH)

    try:
        # Recreate api_keys table with correct schema before migrating data
        recreate_api_keys_table(sqlite_conn)

        for table in TABLES:
            migrate_table(pg_conn, sqlite_conn, table)

    except Exception as e:
        logger.error(f"Unexpected error during migration: {e}")
    finally:
        # Re-enable foreign keys if they were disabled
        if DISABLE_FOREIGN_KEYS:
            try:
                sqlite_conn.execute("PRAGMA foreign_keys = ON;")
                logger.info("Foreign key constraints ENABLED in SQLite.")
            except Exception as e:
                logger.error(f"Failed to enable foreign key constraints: {e}")
        pg_conn.close()
        sqlite_conn.close()
        logger.info("Migration process completed.")

if __name__ == "__main__":
    main()
