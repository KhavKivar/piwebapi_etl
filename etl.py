import pyodbc
import sys
from datetime import datetime, timedelta
import time
from site_param import SITES, SITES_RUN
from sql_param import EVENTFRAME_TEMP_TABLE, EVENTFRAME_TEMP_COLUMNS, SITES_SQL_SDL_LIMIT_Transform,DB_CONFIG,get_map_db_col
from fetch_eventframes import fetch_eventframes

def get_db_connection():
    conn_str_parts = [
        f"DRIVER={DB_CONFIG['driver']};",
        f"SERVER={DB_CONFIG['server']};",
        f"DATABASE={DB_CONFIG['database']};"
    ]
    if 'Trusted_Connection' in DB_CONFIG and DB_CONFIG['Trusted_Connection'].lower() == 'yes':
        conn_str_parts.append("Trusted_Connection=yes;")
    elif 'username' in DB_CONFIG and 'password' in DB_CONFIG:
        conn_str_parts.append(f"UID={DB_CONFIG['username']};")
        conn_str_parts.append(f"PWD={DB_CONFIG['password']};")
    conn_str = "".join(conn_str_parts)
    return pyodbc.connect(conn_str)

def create_eventframe_temp_table(conn):
    cursor = conn.cursor()
    float_cols = {'excursion_value', 'sdlh', 'soldh', 'sdll', 'soll', 'maximum_value', 'minimum_value', 'sdl_limit'}
    datetime_cols = {'start_time', 'end_time', 'last_update', 'start_time_utc', 'end_time_utc'}  # <-- add utc columns here
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
    create_sql = f"IF OBJECT_ID('{EVENTFRAME_TEMP_TABLE}', 'U') IS NOT NULL DROP TABLE {EVENTFRAME_TEMP_TABLE}; " \
                 f"CREATE TABLE {EVENTFRAME_TEMP_TABLE} ({', '.join(columns_sql)});"
    cursor.execute(create_sql)
    conn.commit()
    print(f"Table '{EVENTFRAME_TEMP_TABLE}' created.")

def insert_eventframes(conn, data, current_site):
    import re
    import json
    from dateutil.parser import parse as parse_date
    import time as timer
    cursor = conn.cursor()
    cursor.fast_executemany = True
    float_cols = {'excursion_value', 'sdlh', 'soldh', 'sdll', 'soll', 'maximum_value', 'minimum_value', 'sdl_limit'}
    datetime_cols = {'start_time', 'end_time','start_time_utc','end_time_utc', 'last_update'}  # <-- add utc columns here
    map_db_col = get_map_db_col(current_site)
    placeholders = ", ".join(["?"] * len(EVENTFRAME_TEMP_COLUMNS))
    insert_sql = f"INSERT INTO {EVENTFRAME_TEMP_TABLE} ({', '.join(f'[{col}]' for col in EVENTFRAME_TEMP_COLUMNS)}) VALUES ({placeholders})"
    success_count = 0
    time_when_inserted = datetime.now()
    start_timer = timer.time()
    for idx, row in enumerate(data, 1):
        values = []
        temp_row = {}
        for col in EVENTFRAME_TEMP_COLUMNS:
            if col == 'sdl_limit':
                sdl_limit = None
                for key, value in SITES_SQL_SDL_LIMIT_Transform[current_site].items():
                    if temp_row.get('excursion') == key:
                        sdl_limit = temp_row.get(value)
                        break
                val = sdl_limit
            elif col == 'last_update':
                val = time_when_inserted
            elif col == 'site':
                val = current_site
            else:
                excel_col = map_db_col[col]
                val = row.get(excel_col, None)
                if isinstance(val, str) and val.strip().upper() == 'N/A':
                    val = None
                elif col == 'excursion_type' and isinstance(val, str):
                    try:
                        match = re.match(r'\s*\{.*?"?Name"?\s*:\s*[\'\"](.*?)[\'\"].*?\}', val)
                        if match:
                            val = match.group(1)
                        else:
                            obj = json.loads(val.replace("'", '"'))
                            if isinstance(obj, dict) and 'Name' in obj:
                                val = obj['Name']
                    except Exception:
                        pass
                elif col in float_cols:
                    try:
                        val = float(val) if val not in (None, '', 'N/A') else None
                    except Exception:
                        val = None
                elif col in datetime_cols:
                    val = row.get(map_db_col[col], None)
                    if isinstance(val, str):
                        try:
                            # Remove trailing Z if present
                            val = val.rstrip('Z')
                            # If fractional seconds, keep only up to 6 digits (SQL Server DATETIME2 supports up to 7)
                            if '.' in val:
                                date_part, frac = val.split('.')
                                # Remove any timezone info after fractional seconds
                                frac = ''.join([c for c in frac if c.isdigit()])
                                frac = frac[:6]  # up to microseconds
                                val = f"{date_part}.{frac}"
                            val = parse_date(val)
                        except Exception:
                            val = None
                    elif not isinstance(val, datetime):
                        val = None
                elif col == 'tag_name' and isinstance(val, str):
                    val = re.split(r'[ _]', val)[0]
            temp_row[col] = val
            values.append(val)
        try:
            cursor.execute(insert_sql, values)
            success_count += 1
            if idx % 50 == 0:
                print(f"Inserted {idx} rows...")
        except Exception as e:
            print(f"Error inserting row {idx} with Id={row.get('Id')}: {e}")
    conn.commit()
    end_timer = timer.time()
    elapsed = end_timer - start_timer
    print(f"Inserted {success_count} rows into '{EVENTFRAME_TEMP_TABLE}'. (Attempted {len(data)})")
    print(f"Database write time: {elapsed:.2f} seconds.")

def delete_rows_for_site(conn, site):
    cursor = conn.cursor()
    sql = f"DELETE FROM {EVENTFRAME_TEMP_TABLE} WHERE site = ?"
    try:
        cursor.execute(sql, site)
        conn.commit()
        print(f"Deleted all rows for site '{site}' from table '{EVENTFRAME_TEMP_TABLE}'.")
    except Exception as e:
        print(f"Error deleting rows for site '{site}': {e}")

def get_last_update_time(conn, site):
    cursor = conn.cursor()
    sql = f"SELECT MAX(last_update) FROM {EVENTFRAME_TEMP_TABLE} WHERE site = ?"
    cursor.execute(sql, site)
    result = cursor.fetchone()
    return result[0] if result and result[0] else None

def clean_eventframe_data(data):
    """Remove any row where 'End Time' is '9999-12-31T23:59:59Z'."""
    return [row for row in data if row.get('End Time') != '9999-12-31T19:59:59']

if __name__ == "__main__":
    print(f"--- SQL Server Connection ---")
    print(f"SQL Server: {DB_CONFIG['server']}/{DB_CONFIG['database']}")
    print("-" * 40)
    if len(sys.argv) < 2:
        print("Usage: python etl_to_sql.py [init|populate <site>|run]")
        sys.exit(1)
    command = sys.argv[1].lower()
    conn = get_db_connection()
    if not conn:
        sys.exit(1)
    if command == "init":
        create_eventframe_temp_table(conn)
        print("Table creation complete.")
        conn.close()

    elif command == "populate":
        if len(sys.argv) < 3:
            print("Usage: python etl_to_sql.py populate <site>")
            conn.close()
            sys.exit(1)
        site = sys.argv[2]
        if site not in SITES:
            print(f"Site '{site}' not recognized. Valid sites: {SITES}")
            conn.close()
            sys.exit(1)
        delete_rows_for_site(conn, site)
        print(f"Fetching event frame data for site: {site} via webapi_ETL.py...")
        start_time = '2025-01-01T00:00:00'
        data, _ = fetch_eventframes(site,start_time)
        data = clean_eventframe_data(data)
        print(f"Loaded {len(data)} rows from PI Web API.")
        insert_eventframes(conn, data, site)
        print("Data integration complete.")
        print("\n--- Test Complete ---")
        conn.close()
    elif command == "run":
        try:
            while True:
                for site in SITES_RUN:
                    conn = get_db_connection()
                    print(f"Checking for new events for site: {site}")
                    # Get the last 1 days from now (UTC)
                    start_time = (datetime.utcnow() - timedelta(days=1)).replace(microsecond=0).isoformat()
                    print(f"Fetching events for {site} from: {start_time}")
                    data, _ = fetch_eventframes(site)
                    data = clean_eventframe_data(data)
                    if data:
                        print(f"Loaded {len(data)} rows from PI Web API for {site}.")
                        insert_eventframes(conn, data, site)
                        print(f"Inse rted new events for {site}.")
                    else:
                        print(f"No new events for {site}.")
                    conn.close()
                print("Waiting 20 minutes before next check...")
                time.sleep(1200)
        except KeyboardInterrupt:
            print("Stopped by user.")
        finally:
            conn.close()
    else:
        print("Unknown command. Usage: python etl_to_sql.py [init|populate <site>|run]")
        conn.close()
        sys.exit(1)
