import requests

from urllib.parse import quote_plus
import urllib3
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil.parser import parse as parse_date
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from site_param import SITES_PARAM
import os
from requests_negotiate_sspi import HttpNegotiateAuth
from zoneinfo import ZoneInfo,ZoneInfoNotFoundError
from datetime import datetime, timezone
import json

load_dotenv()

# Suppress SSL warnings for self-signed certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MAX_WORKERS = 20


def to_site_local(utc_dt: datetime, site: str) -> datetime:
    """
    Convert a UTC aware datetime to the site's local time.
    :param utc_dt: datetime with tzinfo=UTC (aware). If naive or non-UTC, raises ValueError.
    :param site: key in SITES_LINK
    :return: aware datetime in the target time zone
    """
    if utc_dt.tzinfo is None:
        raise ValueError("utc_dt is naive; pass an aware UTC datetime (tzinfo=timezone.utc).")
    if utc_dt.tzinfo.utcoffset(utc_dt) != timezone.utc.utcoffset(utc_dt):
        raise ValueError("utc_dt is not in UTC; make sure to pass a UTC datetime.")
    tz_name = SITES_PARAM[site]['TIMEZONE']
    try:
        return utc_dt.astimezone(ZoneInfo(tz_name))
    except ZoneInfoNotFoundError as e:
        raise RuntimeError(
            f"Time zone data not found for '{tz_name}'. Install tzdata: pip install tzdata"
        ) from e
    

# start time utc 
def fetch_eventframes(site, start_time, end_time=datetime.now(timezone.utc),debugMode=False):
    if start_time is None or site is None:
        print("❌ Invalid parameters: start_time and site must be provided.")
        return
    # Only parse if input is a string
    chunk_start = parse_date(start_time) if isinstance(start_time, str) else start_time
    chunk_end = parse_date(end_time) if isinstance(end_time, str) else end_time
    chunk_size = 1000
    all_data = []
    all_attrs = set()
    stack = [(chunk_start, chunk_end)]
    if SITES_PARAM[site]['AUTH'] == 'BASIC':
        AUTH = (os.getenv('PIWEBAPI_USER'), os.getenv('PIWEBAPI_PASS'))
    else:   
        AUTH =  HttpNegotiateAuth() 
    while stack:
        start, end = stack.pop()
        # Ensure both are timezone-aware (UTC)
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        params = {
            'startTime': start.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'endTime': end.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'templateName': SITES_PARAM[site]['TEMPLATE_NAME'],
        }
        query_string = '&'.join([f"{k}={quote_plus(str(v))}" for k, v in params.items()])
        url = f"{SITES_PARAM[site]['PIWEBAPI_URL']}/assetdatabases/{SITES_PARAM[site]['ASSET_DATABASE_WEBID']}/eventframes?{query_string}"
        
        with requests.Session() as session: 
            session.auth = AUTH
            try:
                resp = session.get(url, verify=False, headers={'Accept': 'application/json'})
                if debugMode:
                    print(f"🔗 Fetching Event Frames from: {url}\n    Range: {start} to {end}")
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                if debugMode:
                    print(f"❌ Error fetching Event Frames: {e}")
                continue
            items = data.get('Items', [])
            if len(items) >= chunk_size:
                # Too many items, split the range in half and push both halves to stack
                mid = start + (end - start) / 2
                stack.append((mid, end))
                stack.append((start, mid))
            elif not items:
                continue
            else:
                #Remove eventframe that are still in progress
                items = [ef for ef in items if not (ef.get('End Time') or '').startswith('9999-12-31')]
                if debugMode:
                    print(f"🔍 Found {len(items)} Event Frames. Fetching attributes...\n    Range: {start} to {end}")
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [executor.submit(fetch_attributes, session, ef, AUTH,site,debugMode) for ef in items]
                    for f in as_completed(futures):
                        row_data, attr_names = f.result()
                        all_data.append(row_data)
                        all_attrs.update(attr_names)
    return all_data, all_attrs



def fetch_attributes(session, ef, AUTH, site,debugMode=False):
    # Parse UTC times
    start_time_utc = ef.get('StartTime')
    end_time_utc = ef.get('EndTime')
    # Convert to local time using to_site_local if site is provided
    if site and start_time_utc:
        try:
            start_time_local = to_site_local(parse_date(start_time_utc), site).strftime('%Y-%m-%dT%H:%M:%S')
        except Exception:
            start_time_local = ''
    else:
        start_time_local = ''
    if site and end_time_utc:
        try:
            end_time_local = to_site_local(parse_date(end_time_utc), site).strftime('%Y-%m-%dT%H:%M:%S')
        except Exception:
            end_time_local = ''
    else:
        end_time_local = ''
    
    ef_row_data = {
        'Event Frame Name': ef.get('Name'),
        'Start Time': str(start_time_local),      # Local time
        'Start Time UTC': start_time_utc,         # UTC
        'End Time': str(end_time_local),          # Local time
        'End Time UTC': end_time_utc,             # UTC
        'Template Name': ef.get('TemplateName'),
        'WebId': ef.get('WebId'),
        'Id': ef.get('Id')
    }
    
    attributes_link = ef.get('Links', {}).get('Attributes')
    if not attributes_link:
        if debugMode:
            print(f"✔️  Event Frame '{ef.get('Name')}' has no attributes link.")
        return ef_row_data, set()

    attribute_names = set()

    try:
        attr_resp = session.get(attributes_link, auth=AUTH, verify=False, headers={'Accept': 'application/json'})
        attr_resp.raise_for_status()
        attr_data = attr_resp.json()

        if attr_data and 'Items' in attr_data:
            attr_items = attr_data['Items']

            def fetch_value(attr):
                name = attr.get('Name')
                value_link = attr.get('Links', {}).get('Value')
                try:
                    if value_link:
                        val_resp = session.get(value_link, auth=AUTH, verify=False, headers={'Accept': 'application/json'})
                        val_resp.raise_for_status()
                        val_data = val_resp.json()
                        actual_value = val_data.get('Value')
                    else:
                        actual_value = attr.get('Value')

                    if isinstance(actual_value, (dict, list)):
                        value_to_store = json.dumps(actual_value)
                    else:
                        value_to_store = actual_value

                    return [(name, value_to_store)]
                except Exception as e:
                    return [(name, "")]

            # Fetch all attribute values in parallel
            with ThreadPoolExecutor(max_workers=5) as value_executor:
                future_value_tasks = [value_executor.submit(fetch_value, attr) for attr in attr_items]
                for f in as_completed(future_value_tasks):
                    results = f.result()
                    for key, val in results:
                        ef_row_data[key] = val
                        attribute_names.add(key)
            if debugMode:
                print(f"✅ Successfully fetched all attributes for Event Frame: '{ef.get('Name')}'")

    except Exception as e:
        if debugMode:
            print(ef)
            print(f"❌ Error fetching attributes for EF '{ef.get('Name')}': {e}")

    return ef_row_data, attribute_names


# --- Run ---
if __name__ == "__main__":
    print(to_site_local(datetime(2025, 6, 30, 19, 56, 40, 23000, tzinfo=timezone.utc),'EGYPT')) 