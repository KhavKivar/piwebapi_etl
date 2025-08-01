import pyodbc
import pandas as pd
from zoneinfo import ZoneInfo


from sql_param import (
    EVENTFRAME_TEMP_TABLE,
    EVENTFRAME_TEMP_COLUMNS,
    SITES_SQL_SDL_LIMIT_Transform,
    DB_CONFIG,
    get_map_db_col
)

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

if __name__ == "__main__":
    table = 'dbo.geismar_merged'
    query = f"SELECT * FROM {table}"
    
    try:
        with get_db_connection() as conn:
            df = pd.read_sql(query, conn)
            df['start_time'] = pd.to_datetime(df['start_time_pi'], utc=True).dt.tz_convert('America/Punta_Arenas').dt.tz_localize(None)
            df['end_time'] = pd.to_datetime(df['end_time_pi'], utc=True).dt.tz_convert('America/Punta_Arenas').dt.tz_localize(None)

            df = df.sort_values(by='start_time', ascending=True)
            # Keep only these columns
            print(df)
          

            print(f"✅ Successfully loaded {len(df)} rows from {table}")
    except Exception as e:
        print(f"❌ Error: {e}")
        exit()

    output_file = f"33.xlsx"
    try:
        df.to_excel(output_file, index=False, engine='openpyxl')
        print(f"📁 Data written to Excel file: {output_file}")
    except Exception as e:
        print(f"❌ Failed to write to Excel: {e}")
