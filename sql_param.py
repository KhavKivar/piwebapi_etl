

DB_CONFIG = {
    'server': 'usgei-dbts01',  
    'database': 'sdlsolexcursions',      
    'Trusted_Connection': 'yes',             
    'driver': '{ODBC Driver 17 for SQL Server}' 
}


# --- Table/Column Definitions ---
EVENTFRAME_TEMP_TABLE = 'eventframe_cache'

EVENTFRAME_TEMP_COLUMNS = [
    'event_frame_name', 'start_time', 'end_time','start_time_utc','end_time_utc','template_name', 'webid', 'id', 'description', 'excursion',
    'excursion_duration', 'excursion_type', 'excursion_value', 'sdlh', 'soldh', 'sdll', 'soll',
    'maximum_value', 'minimum_value', 'plant', 'tag_name', 'type', 'url', 'units', 'sdl_limit',
    'last_update', 'site'
]
SITES_SQL_Transform = {
    'TRINIDAD': {
        'sdlh': 'Hi',
        'soldh': 'HiHi',
        'sdll': 'Lo',
        'soll': 'LoLo',
    },
    'CHILE': {
        'sdlh': 'SDLH',
        'soldh': 'SOLH',
        'sdll': 'SDLL',
        'soll': 'SOLL',
    },
    'MEDICINE HAT': {
        'sdlh': 'SDLH',
        'soldh': 'SOLH',
        'sdll': 'SDLL',
        'soll': 'SOLL',
    },
    'EGYPT': {
        'sdlh': 'SDLH',
        'soldh': 'SOLH',
        'sdll': 'SDLL',
        'soll': 'SOLL'
    }
}
SITES_SQL_SDL_LIMIT_Transform = {
    'TRINIDAD': {
        '> High Limit': 'sdlh',
        '< Low Limit': 'sdll'
    },
    'CHILE': {
        '> SDLH': 'sdlh',
        '> SOLH': 'soldh',
        '< SOLL': 'soll',
    },
    'MEDICINE HAT': {
        '> SDLH': 'sdlh',
        '> SOLH': 'soldh',
        '< SOLL': 'soll',
    },
    'EGYPT': {
        '> SDLH': 'sdlh',
        '> SOLH': 'soldh',
        '< SOLL': 'soll',
        '< SDLL': 'sdll'
    }
}

def get_map_db_col(current_site):
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
        'sdlh': SITES_SQL_Transform[current_site]['sdlh'],
        'soldh': SITES_SQL_Transform[current_site]['soldh'],
        'sdll': SITES_SQL_Transform[current_site]['sdll'],
        'soll': SITES_SQL_Transform[current_site]['soll'],
        'maximum_value': 'Maximum value',
        'minimum_value': 'Minimum value',
        'plant': 'Plant',
        'tag_name': 'Tag name',
        'type': 'Type',
        'url': 'URL',
        'units': 'Units',
        'sdl_limit': None
    }