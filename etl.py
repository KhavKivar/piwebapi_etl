"""
ETL Pipeline for PI Web API Event Frame Data
============================================

This module handles the extraction, transformation, and loading of event frame data
from PI Web API sources into SQL Server database.

Main Features:
- Fetch event frames from multiple sites
- Transform data according to site-specific configurations
- Load data into SQL Server with batch processing
- Export data to CSV format
- Support for both fast and slow insert methods

Usage:
    python etl.py init                    # Initialize database tables
    python etl.py populate <site>         # Populate data for specific site
    python etl.py workbook <site>         # Export data to CSV
    python etl.py test                    # Run test with sample data
    python etl.py run                     # Continuous monitoring mode
"""

import pyodbc
import sys
import json
import os
import csv
import re
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse as parse_date
import time as timer

from fetch_eventframes import fetch_eventframes

# =============================================================================
# CONSTANTS
# =============================================================================

EVENTFRAME_TEMP_TABLE_TEST = "eventframe_test"

# =============================================================================
# CONFIGURATION MANAGEMENT
# =============================================================================

def load_config():
    """Load configuration from JSON file."""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file: {e}")

# Load global configuration
CONFIG = load_config()
SITES = CONFIG['sites_list']
SITES_RUN = CONFIG['sites_run']
SITES_PARAM = CONFIG['sites']
EVENTFRAME_TEMP_TABLE = CONFIG['tables']['eventframe_temp_table']
EVENTFRAME_TEMP_COLUMNS = CONFIG['tables']['eventframe_temp_columns']
DB_CONFIG = CONFIG['database']

# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def get_db_connection():
    """
    Create and return a database connection using configuration settings.
    
    Returns:
        pyodbc.Connection: Database connection object
    """
    conn_str_parts = [
        f"DRIVER={DB_CONFIG['driver']};",
        f"SERVER={DB_CONFIG['server']};",
        f"DATABASE={DB_CONFIG['database']};"
    ]
    
    # Add authentication method
    if 'Trusted_Connection' in DB_CONFIG and DB_CONFIG['Trusted_Connection'].lower() == 'yes':
        conn_str_parts.append("Trusted_Connection=yes;")
    elif 'username' in DB_CONFIG and 'password' in DB_CONFIG:
        conn_str_parts.append(f"UID={DB_CONFIG['username']};")
        conn_str_parts.append(f"PWD={DB_CONFIG['password']};")
    
    conn_str = "".join(conn_str_parts)
    return pyodbc.connect(conn_str)

def create_eventframe_temp_table(conn, table_name=None):
    """
    Create the event frame temporary table with proper column types.
    
    Args:
        conn: Database connection
        table_name: Optional table name override
    """
    if table_name is None:
        table_name = EVENTFRAME_TEMP_TABLE
    
    cursor = conn.cursor()
    
    # Define column types
    float_cols = {
        'excursion_value', 'sdlh', 'soldh', 'sdll', 'soll', 
        'maximum_value', 'minimum_value', 'sdl_limit'
    }
    datetime_cols = {
        'start_time', 'end_time', 'last_update', 'start_time_utc', 'end_time_utc'
    }
    
    # Build column definitions
    columns_sql = []
    for col in EVENTFRAME_TEMP_COLUMNS:
        if col == 'id':
            columns_sql.append("[id] NVARCHAR(100) PRIMARY KEY")
        elif col in float_cols:
            columns_sql.append(f"[{col}] FLOAT NULL")
        elif col in datetime_cols:
            columns_sql.append(f"[{col}] DATETIME NULL")
        else:
            columns_sql.append(f"[{col}] NVARCHAR(MAX) NULL")
    
    # Create table SQL
    create_sql = (
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}; "
        f"CREATE TABLE {table_name} ({', '.join(columns_sql)});"
    )
    
    cursor.execute(create_sql)
    conn.commit()
    print(f"✅ Table '{table_name}' created successfully.")

def create_run_tracking_table(conn):
    """
    Create the run tracking table to store last run times for each site.
    Only creates if it doesn't exist.
    
    Args:
        conn: Database connection
    """
    cursor = conn.cursor()
    
    # Create table SQL - only if it doesn't exist
    create_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='etl_run_tracking' AND xtype='U')
    BEGIN
        CREATE TABLE etl_run_tracking (
            site NVARCHAR(50) PRIMARY KEY,
            last_run_time DATETIME NOT NULL,
            created_at DATETIME DEFAULT GETDATE(),
            updated_at DATETIME DEFAULT GETDATE()
        )
    END
    """
    
    cursor.execute(create_sql)
    conn.commit()
    print("✅ Run tracking table ready.")

def get_last_run_time(conn, site):
    """
    Get the last run time for a specific site.
    
    Args:
        conn: Database connection
        site: Site name
        
    Returns:
        datetime or None: Last run time or None if no previous run
    """
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT last_run_time FROM etl_run_tracking WHERE site = ?", site)
        result = cursor.fetchone()
        
        if result:
            last_run_time = result[0]
            print(f"📅 Last run time for {site}: {last_run_time} UTC")
            return last_run_time
        else:
            print(f"📅 No previous run found for {site}")
            return None
            
    except Exception as e:
        print(f"❌ Error getting last run time for {site}: {e}")
        return None

def update_last_run_time(conn, site, run_time):
    """
    Update or insert the last run time for a specific site.
    
    Args:
        conn: Database connection
        site: Site name
        run_time: DateTime of the run
    """
    cursor = conn.cursor()
    
    try:
        # Use MERGE to insert or update
        merge_sql = """
        MERGE etl_run_tracking AS target
        USING (SELECT ? AS site, ? AS last_run_time) AS source
        ON target.site = source.site
        WHEN MATCHED THEN
            UPDATE SET last_run_time = source.last_run_time, updated_at = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (site, last_run_time) VALUES (source.site, source.last_run_time);
        """
        
        cursor.execute(merge_sql, site, run_time)
        conn.commit()
        print(f"✅ Updated last run time for {site}: {run_time} UTC")
        
    except Exception as e:
        print(f"❌ Error updating last run time for {site}: {e}")

def delete_rows_for_site(conn, site, table_name=None):
    """
    Delete all rows for a specific site from the table.
    
    Args:
        conn: Database connection
        site: Site name to delete
        table_name: Optional table name override
    """
    if table_name is None:
        table_name = EVENTFRAME_TEMP_TABLE
    
    cursor = conn.cursor()
    sql = f"DELETE FROM {table_name} WHERE site = ?"
    
    try:
        cursor.execute(sql, site)
        conn.commit()
        print(f"🗑️  Deleted all rows for site '{site}' from table '{table_name}'.")
    except Exception as e:
        print(f"❌ Error deleting rows for site '{site}': {e}")

# =============================================================================
# DATA TRANSFORMATION
# =============================================================================

def get_map_db_col(current_site):
    """
    Get column mapping for a specific site.
    
    Args:
        current_site: Site name
        
    Returns:
        dict: Mapping of database columns to source columns
    """
    site_config = CONFIG['sites'][current_site]
    
    return {
        'event_frame_name': 'Event Frame Name',
        'start_time': 'Start Time',
        'end_time': 'End Time',
        'start_time_utc': 'Start Time UTC',
        'end_time_utc': 'End Time UTC',
        'template_name': 'Template Name',
        'webid': 'WebId',
        'id': 'Id',
        'description': 'Description',
        'excursion': 'Excursion',
        'excursion_duration': 'Excursion duration',
        'excursion_type': 'Excursion type',
        'excursion_value': 'Excursion value',
        'sdlh': site_config['sql_transform']['sdlh'],
        'soldh': site_config['sql_transform']['soldh'],
        'sdll': site_config['sql_transform']['sdll'],
        'soll': site_config['sql_transform']['soll'],
        'maximum_value': 'Maximum value',
        'minimum_value': 'Minimum value',
        'plant': 'Plant',
        'tag_name': 'Tag name',
        'type': 'Type',
        'url': 'URL',
        'units': 'Units',
        'sdl_limit': None
    }

def process_row_data(row, current_site, time_when_inserted):
    """
    Process a single row of data and return values for database insertion.
    
    Args:
        row: Raw data row from PI Web API
        current_site: Site name for configuration lookup
        time_when_inserted: Timestamp for last_update field
        
    Returns:
        list: Processed values ready for database insertion
    """
    # Define column types for proper conversion
    float_cols = {
        'excursion_value', 'sdlh', 'soldh', 'sdll', 'soll', 'maximum_value', 
        'minimum_value', 'sdl_limit', 'excursion_duration'
    }
    datetime_cols = {
        'start_time', 'end_time', 'start_time_utc', 'end_time_utc', 'last_update'
    }
    
    map_db_col = get_map_db_col(current_site)
    temp_row = {}
    row_values = []
    
    for col in EVENTFRAME_TEMP_COLUMNS:
        val = None
        
        # Handle special columns
        if col == 'sdl_limit':
            val = _calculate_sdl_limit(temp_row, current_site)
        elif col == 'last_update':
            val = time_when_inserted
        elif col == 'site':
            val = current_site
        else:
            # Get value from source data
            excel_col = map_db_col[col]
            val = row.get(excel_col, None)
            
            # Apply transformations based on column type
            val = _transform_value(val, col, float_cols, datetime_cols, map_db_col, row)
        
        temp_row[col] = val
        row_values.append(val)
    
    return row_values

def _calculate_sdl_limit(temp_row, current_site):
    """Calculate SDL limit based on excursion type."""
    sdl_limit = None
    sdl_transform = CONFIG['sites'][current_site]['sdl_limit_transform']
    
    for key, value in sdl_transform.items():
        if temp_row.get('excursion') == key:
            sdl_limit = temp_row.get(value)
            break
    
    return sdl_limit

def _transform_value(val, col, float_cols, datetime_cols, map_db_col, row):
    """Transform a single value based on its column type."""
    if isinstance(val, str) and val.strip().upper() == 'N/A':
        return None
    
    if col == 'excursion_type' and isinstance(val, str):
        return _parse_excursion_type(val)
    elif col in float_cols:
        return _parse_float_value(val)
    elif col in datetime_cols:
        return _parse_datetime_value(row.get(map_db_col[col], None))
    elif col == 'tag_name' and isinstance(val, str):
        return re.split(r'[ _]', val)[0]
    
    return val

def _parse_excursion_type(val):
    """Parse excursion type from JSON-like string."""
    try:
        match = re.match(r'\s*\{.*?"?Name"?\s*:\s*[\'\"](.*?)[\'\"].*?\}', val)
        if match:
            return match.group(1)
        else:
            obj = json.loads(val.replace("'", '"'))
            if isinstance(obj, dict) and 'Name' in obj:
                return obj['Name']
    except Exception:
        pass
    return val

def _parse_float_value(val):
    """Parse float value with error handling."""
    try:
        return float(val) if val not in (None, '', 'N/A') else None
    except Exception:
        return None

def _parse_datetime_value(val):
    """Parse datetime value with timezone handling."""
    if isinstance(val, str):
        try:
            val = val.rstrip('Z')
            if '.' in val:
                date_part, frac = val.split('.')
                frac = ''.join([c for c in frac if c.isdigit()])
                frac = frac[:6]
                val = f"{date_part}.{frac}"
            return parse_date(val)
        except Exception:
            return None
    elif not isinstance(val, datetime):
        return None
    return val

# =============================================================================
# DATA LOADING
# =============================================================================

def insert_eventframes(conn, data, current_site, table_name=None, use_fast_insert=True):
    """
    Insert event frame data into database with optional batch processing.
    
    Args:
        conn: Database connection
        data: List of event frame records
        current_site: Site name
        table_name: Optional table name override
        use_fast_insert: Whether to use fast batch insert (default: True)
    """
    if not data:
        print("⚠️  No data to insert")
        return
    
    if table_name is None:
        table_name = EVENTFRAME_TEMP_TABLE

    cursor = conn.cursor()
    if use_fast_insert:
        cursor.fast_executemany = True

    # Prepare SQL statement
    placeholders = ", ".join(["?"] * len(EVENTFRAME_TEMP_COLUMNS))
    insert_sql = (
        f"INSERT INTO {table_name} "
        f"({', '.join(f'[{col}]' for col in EVENTFRAME_TEMP_COLUMNS)}) "
        f"VALUES ({placeholders})"
    )
    
    success_count = 0
    time_when_inserted = datetime.now()
    start_timer = timer.time()
    
    print(f"📊 Processing {len(data)} records for site: {current_site}")

    if use_fast_insert:
        success_count = _fast_batch_insert(cursor, conn, data, current_site, 
                                         time_when_inserted, insert_sql)
    else:
        success_count = _slow_individual_insert(cursor, conn, data, current_site, 
                                              time_when_inserted, insert_sql)
    
    # Report results
    end_timer = timer.time()
    elapsed = end_timer - start_timer
    method = "FAST batch" if use_fast_insert else "SLOW individual"
    
    print(f"✅ Inserted {success_count} rows into '{table_name}' using {method} insert.")
    print(f"⏱️  Database write time: {elapsed:.2f} seconds.")

def _fast_batch_insert(cursor, conn, data, current_site, time_when_inserted, insert_sql):
    """Perform fast batch insert operation."""
    data_parse = []
    
    for idx, row in enumerate(data, 1):
        row_values = process_row_data(row, current_site, time_when_inserted)
        data_parse.append(row_values)
        
        if idx % 100 == 0:
            print(f"  Processed {idx} rows...")

    try:
        cursor.executemany(insert_sql, data_parse)
        conn.commit()
        return len(data_parse)
    except Exception as e:
        print(f"❌ Error during batch insert: {e}")
        raise

def _slow_individual_insert(cursor, conn, data, current_site, time_when_inserted, insert_sql):
    """Perform slow individual insert operation."""
    success_count = 0
    
    for idx, row in enumerate(data, 1):
        try:
            row_values = process_row_data(row, current_site, time_when_inserted)
            cursor.execute(insert_sql, row_values)
            success_count += 1
            
            if idx % 50 == 0:
                print(f"  Processed {idx} rows...")
                conn.commit()  # Commit every 50 rows
                
        except Exception as e:
            print(f"❌ Error inserting row {idx}: {e}")
            continue

    # Final commit
    try:
        conn.commit()
        return success_count
    except Exception as e:
        print(f"❌ Error during final commit: {e}")
        raise

# =============================================================================
# CSV EXPORT
# =============================================================================

def process_eventframes_to_csv(data, current_site, csv_filename=None):
    """
    Process event frame data and export to CSV format.
    
    Args:
        data: List of event frame records
        current_site: Site name
        csv_filename: Optional CSV filename
    """
    if csv_filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"eventframes_{current_site.lower()}_{timestamp}.csv"
    
    print(f"📄 Exporting {len(data)} records to CSV: {csv_filename}")
    
    success_count = 0
    time_when_processed = datetime.now()
    start_timer = timer.time()
    processed_data = []
    
    # Process each row
    for idx, row in enumerate(data, 1):
        temp_row = {}
        
        for col in EVENTFRAME_TEMP_COLUMNS:
            if col == 'sdl_limit':
                val = _calculate_sdl_limit(temp_row, current_site)
            elif col == 'last_update':
                val = time_when_processed
            elif col == 'site':
                val = current_site
            else:
                map_db_col = get_map_db_col(current_site)
                excel_col = map_db_col[col]
                val = row.get(excel_col, None)
                
                # Apply same transformations as database insert
                float_cols = {
                    'excursion_value', 'sdlh', 'soldh', 'sdll', 'soll', 
                    'maximum_value', 'minimum_value', 'sdl_limit', 'excursion_duration'
                }
                datetime_cols = {
                    'start_time', 'end_time', 'start_time_utc', 'end_time_utc', 'last_update'
                }
                val = _transform_value(val, col, float_cols, datetime_cols, map_db_col, row)
            
            temp_row[col] = val
        
        processed_data.append(temp_row)
        success_count += 1
        
        if idx % 100 == 0:
            print(f"  Processed {idx} rows...")
    
    # Write to CSV file
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            if processed_data:
                writer = csv.DictWriter(csvfile, fieldnames=EVENTFRAME_TEMP_COLUMNS)
                writer.writeheader()
                writer.writerows(processed_data)
        
        end_timer = timer.time()
        elapsed = end_timer - start_timer
        
        print(f"✅ Exported {success_count} rows to '{csv_filename}'")
        print(f"⏱️  CSV write time: {elapsed:.2f} seconds.")
        print(f"📁 File saved at: {os.path.abspath(csv_filename)}")
        
    except Exception as e:
        print(f"❌ Error writing to CSV: {e}")
        raise

# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

def print_usage():
    """Print usage instructions."""
    print("Usage: python etl.py [command] [options]")
    print()
    print("Commands:")
    print("  init                     Initialize database tables")
    print("  populate <site>          Populate data for specific site")
    print("  workbook <site>          Export data to CSV for specific site")
    print("  test                     Run test with sample data")
    print("  run                      Continuous monitoring mode")
    print()
    print(f"Available sites: {', '.join(SITES)}")

def main():
    """Main entry point for command line interface."""
    print("=" * 60)
    print("PI WEB API ETL PIPELINE")
    print("=" * 60)
    print(f"🔗 SQL Server: {DB_CONFIG['server']}/{DB_CONFIG['database']}")
    print(f"📋 Available Sites: {', '.join(SITES)}")
    print("-" * 60)
    
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    try:
        if command == "init":
            conn = get_db_connection()
            create_eventframe_temp_table(conn)
            print("✅ Table creation complete.")
            conn.close()

        elif command == "populate":
            if len(sys.argv) < 3:
                print("❌ Error: Site name required")
                print("Usage: python etl.py populate <site>")
                sys.exit(1)
                
            site = sys.argv[2].upper()
            if site not in SITES:
                print(f"❌ Error: Site '{site}' not recognized. Valid sites: {SITES}")
                sys.exit(1)
            
            conn = get_db_connection()
            delete_rows_for_site(conn, site)
            
            print(f"🔄 Fetching event frame data for site: {site}")
            start_time = '2025-01-01T00:00:00'
            data, _ = fetch_eventframes(site, start_time, datetime.now(timezone.utc), True)
            
            print(f"📊 Loaded {len(data)} rows from PI Web API.")
            insert_eventframes(conn, data, site)
            print("✅ Data integration complete.")
            conn.close()

        elif command == "workbook":
            if len(sys.argv) < 3:
                print("❌ Error: Site name required")
                print("Usage: python etl.py workbook <site>")
                sys.exit(1)
                
            site = sys.argv[2].upper()
            if site not in SITES:
                print(f"❌ Error: Site '{site}' not recognized. Valid sites: {SITES}")
                sys.exit(1)
            
            print(f"🔄 Fetching event frame data for site: {site}")
            
            # Define date range (last 6 months)
            start_date = datetime(2025, 7, 1, tzinfo=timezone.utc)
            end_date = datetime.now(timezone.utc)
            total_days = (end_date - start_date).days
            
            print(f"📅 Processing {total_days} days from {start_date.strftime('%Y-%m-%d')} "
                  f"to {end_date.strftime('%Y-%m-%d')}")
            
            # Fetch data
            batch_start = start_date.strftime('%Y-%m-%dT00:00:00')
            batch_end = end_date.strftime('%Y-%m-%dT00:00:00')
            
            data, _ = fetch_eventframes(site, batch_start, batch_end, True)
            
            if data:
                # Generate CSV with date range
                start_str = start_date.strftime('%Y%m%d')
                end_str = (end_date - timedelta(days=1)).strftime('%Y%m%d')
                csv_filename = f"eventframes_{site.lower()}_{start_str}_to_{end_str}.csv"
                
                process_eventframes_to_csv(data, site, csv_filename)
                print("✅ CSV export complete.")
            else:
                print("⚠️  No data to export for the specified date range.")

        elif command == "test":
            site = "CHILE"
            print(f"🧪 [TESTING] Running ETL for site: {site}")
            
            conn = get_db_connection()
            create_eventframe_temp_table(conn, table_name=EVENTFRAME_TEMP_TABLE_TEST)
            
            print(f"🔄 Fetching test data...")
            start_time = '2025-01-01T00:00:00'
            data, _ = fetch_eventframes(site, start_time, datetime.now(timezone.utc), True)
            
            print(f"📊 Loaded {len(data)} test rows from PI Web API.")
            insert_eventframes(conn, data, site, table_name=EVENTFRAME_TEMP_TABLE_TEST)
            print("✅ Test complete.")
            conn.close()

        elif command == "run":
            print("🔄 Starting continuous monitoring mode...")
            print("Press Ctrl+C to stop")
            
            # Initialize tracking table once at the start
            init_conn = None
            try:
                init_conn = get_db_connection()
                create_run_tracking_table(init_conn)
            except Exception as init_e:
                print(f"❌ Error initializing tracking table: {init_e}")
            finally:
                if init_conn:
                    init_conn.close()
            
            while True:  # Outer loop to ensure continuous operation
                try:
                    current_run_time = datetime.now(timezone.utc)
                    
                    for site in SITES_RUN:
                        print(f"🔍 Checking for new events for site: {site}")
                        
                        # Try to connect to database with retry logic
                        conn = None
                        max_db_retries = 3
                        for db_attempt in range(1, max_db_retries + 1):
                            try:
                                print(f"🔗 Attempting database connection for {site} (attempt {db_attempt}/{max_db_retries})...")
                                conn = get_db_connection()
                                print(f"✅ Database connection successful for {site}")
                                break
                            except Exception as db_e:
                                if db_attempt < max_db_retries:
                                    print(f"❌ Database connection attempt {db_attempt} failed: {db_e}")
                                    print(f"⏳ Waiting 5 seconds before retry...")
                                    timer.sleep(5)
                                else:
                                    print(f"❌ All {max_db_retries} database connection attempts failed for {site}")
                                    print(f"❌ Skipping {site} and continuing with next site...")
                                    conn = None
                                    break
                        
                        # Only proceed if we have a valid connection
                        if conn is not None:
                            try:
                                # Get last run time or use default lookback
                                last_run = get_last_run_time(conn, site)
                                
                                if last_run:
                                    # Use last run time as start time
                                    start_time = last_run.replace(microsecond=0).isoformat()
                                    print(f"📅 Using last run time as start: {start_time}")
                                else:
                                    # First run - look back 15 days
                                    start_time = (current_run_time - timedelta(hours=360)).replace(microsecond=0).isoformat()
                                    print(f"📅 First run - looking back 15 days: {start_time}")
                                
                                data, _ = fetch_eventframes(site, start_time)
                                
                                if data:
                                    print(f"📊 Found {len(data)} new rows for {site}")
                                    insert_eventframes(conn, data, site)
                                    print(f"✅ Inserted new events for {site}")
                                else:
                                    print(f"ℹ️  No new events for {site}")
                                
                                # Update last run time for this site (regardless of data found)
                                update_last_run_time(conn, site, current_run_time)
                                
                            except Exception as site_e:
                                print(f"❌ Error processing site {site}: {site_e}")
                                print(f"⏭️  Continuing with next site...")
                            finally:
                                # Always close the connection
                                try:
                                    conn.close()
                                    print(f"🔌 Database connection closed for {site}")
                                except Exception as close_e:
                                    print(f"⚠️  Warning: Error closing connection for {site}: {close_e}")
                        else:
                            print(f"⏭️  Skipping {site} due to connection failure")
                    
                    print("⏰ Waiting 60 minutes before next check...")
                    timer.sleep(3600)
                    
                except KeyboardInterrupt:
                    print("\n🛑 Stopped by user.")
                    break  # Exit the outer loop only for user interruption
                except Exception as e:
                    print(f"❌ Unexpected error in continuous mode: {e}")
                    print("🔄 Attempting to recover and continue in 30 seconds...")
                    timer.sleep(30)  # Wait 30 seconds before retrying the entire cycle
                    continue  # Continue the outer while loop

        else:
            print(f"❌ Unknown command: {command}")
            print_usage()
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
