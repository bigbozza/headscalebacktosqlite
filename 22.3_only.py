import shutil
import psycopg2
import sqlite3
import os
from datetime import datetime
import argparse

def copy_sqlite_db(source='og.sqlite.db', destination='db.sqlite'):
    """Copy the original SQLite database to a new file."""
    try:
        shutil.copy(source, destination)
        print(f"Database copied successfully from {source} to {destination}")
        return True
    except Exception as e:
        print(f"Error copying database: {e}")
        return False

def connect_to_postgres(host, database, user, password, port=5432):
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=host, database=database, user=user, password=password, port=port
        )
        print(f"Connected to PostgreSQL database {database}")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def connect_to_sqlite(db_file='db.sqlite'):
    """Connect to the SQLite database."""
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to SQLite database {db_file}")
        return conn
    except Exception as e:
        print(f"Error connecting to SQLite: {e}")
        return None

def get_postgres_table_info(pg_conn, table_name, schema):
    """Get column information for a PostgreSQL table."""
    cursor = pg_conn.cursor()
    cursor.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    columns_info = cursor.fetchall()
    cursor.close()
    return columns_info

def get_postgres_data(pg_conn, table_name, schema):
    """Get all data from a PostgreSQL table."""
    # Get column info first
    columns_info = get_postgres_table_info(pg_conn, table_name, schema)
    column_names = [col[0] for col in columns_info]
    column_types = [col[1] for col in columns_info]

    # Get data
    cursor = pg_conn.cursor()
    cursor.execute(f"SELECT * FROM {schema}.{table_name}")
    data = cursor.fetchall()
    cursor.close()

    # Process data based on column types
    processed_data = []
    for row in data:
        processed_row = []
        for i, value in enumerate(row):
            # Handle special data types
            if column_types[i] == 'boolean':
                # Convert boolean to 0 or 1 for SQLite
                processed_row.append(1 if value else 0 if value is not None else None)
            elif 'timestamp' in column_types[i]:
                # Special handling for timestamps
                if value is not None:
                    # Special handling for expiry field
                    if column_names[i] == 'expiry':
                        # Check if this is a "zero date" (year 1)
                        if value.year == 1:
                            # Format as special zero date for SQLite
                            processed_row.append('0001-01-01 00:00:00+00:00')
                        else:
                            # Format regular dates with timezone info
                            # Strip space before timezone if present
                            formatted = value.isoformat(' ').replace(' +', '+')
                            processed_row.append(formatted)
                    else:
                        # For other timestamp fields, maintain as standard format
                        formatted = value.isoformat(' ').replace(' +', '+')
                        processed_row.append(formatted)
                else:
                    # Null values stay as None
                    processed_row.append(None)
            else:
                processed_row.append(value)
        processed_data.append(tuple(processed_row))

    return processed_data, column_names

def clear_sqlite_table(sqlite_conn, table_name):
    """Clear all data from an SQLite table."""
    cursor = sqlite_conn.cursor()
    cursor.execute(f"DELETE FROM `{table_name}`")
    sqlite_conn.commit()
    cursor.close()
    print(f"Cleared data from SQLite table {table_name}")

def insert_into_sqlite(sqlite_conn, table_name, columns, data):
    """Insert data into an SQLite table."""
    if not data:
        print(f"No data to insert into SQLite table {table_name}")
        return False
    
    cursor = sqlite_conn.cursor()
    
    # Create placeholders for the SQL query
    placeholders = ", ".join(["?" for _ in columns])
    columns_str = ", ".join([f"`{col}`" for col in columns])
    
    # Insert data
    cursor.executemany(
        f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})",
        data
    )
    
    sqlite_conn.commit()
    print(f"Inserted {len(data)} rows into SQLite table {table_name}")
    cursor.close()
    return True

def get_postgres_tables(pg_conn, schema):
    """Get list of tables in PostgreSQL schema."""
    cursor = pg_conn.cursor()
    cursor.execute(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'
    """)
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    return tables

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Import data from PostgreSQL to SQLite')
    parser.add_argument('schema', help='PostgreSQL schema/namespace')
    parser.add_argument('password', help='PostgreSQL password')
    parser.add_argument('server', nargs='?', default='localhost', help='PostgreSQL server URL (default: localhost)')
    parser.add_argument('database', nargs='?', default='postgres', help='PostgreSQL database name (default: postgres)')
    args = parser.parse_args()
    
    # Step 1: Copy the SQLite database
    if not copy_sqlite_db():
        print("Failed to copy database. Proceeding with import anyway.")
    
    # Step 2: Connect to databases
    pg_conn = connect_to_postgres(
        host=args.server, 
        database=args.database, 
        user=args.schema, 
        password=args.password
    )
    
    if not pg_conn:
        print("Failed to connect to PostgreSQL. Exiting.")
        return
    
    sqlite_conn = connect_to_sqlite()
    if not sqlite_conn:
        print("Failed to connect to SQLite. Exiting.")
        pg_conn.close()
        return
    
    # Step 3: Get list of tables from PostgreSQL
    tables = get_postgres_tables(pg_conn, args.schema)
    
    # Step 4: Process each table
    for table in tables:
        print(f"\nProcessing table: {table}")
        
        # Get data from PostgreSQL
        data, columns = get_postgres_data(pg_conn, table, args.schema)
        
        if not data:
            print(f"No data found in PostgreSQL table {table}")
            continue
        
        # Clear existing data in SQLite
        try:
            clear_sqlite_table(sqlite_conn, table)
            
            # Insert data into SQLite
            insert_into_sqlite(sqlite_conn, table, columns, data)
        except sqlite3.OperationalError as e:
            print(f"Error processing table {table}: {e}")
    
    # Step 5: Close connections
    pg_conn.close()
    sqlite_conn.close()
    print("\nData import completed")

if __name__ == "__main__":
    main()
