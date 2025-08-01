"""
PI Web API Event Frame Fetcher
==============================

Thread-safe event frame and attribute fetching from PI Web API sources.

Key Features:
- Fetches event frames with recursive chunking for large datasets
- Thread-safe attribute retrieval using isolated sessions
- Timezone conversion to site-local time
- Comprehensive error handling and debug logging
- Filters out in-progress events automatically

Usage:
    from fetch_eventframes import fetch_eventframes
    
    data, attrs = fetch_eventframes('SITE_NAME', 
                                   '2025-01-01T00:00:00Z', 
                                   '2025-01-31T23:59:59Z', 
                                   debugMode=True)
"""

from __future__ import annotations

import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Set, Tuple

import requests
import urllib3
from urllib.parse import quote_plus
from dateutil.parser import parse as parse_date
from dotenv import load_dotenv
from requests_negotiate_sspi import HttpNegotiateAuth
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Optional Excel helpers (imported by caller code)
from openpyxl.utils import get_column_letter  # noqa: F401
from openpyxl.styles import Alignment  # noqa: F401

# =============================================================================
# CONFIGURATION
# =============================================================================

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Threading configuration
MAX_WORKERS = 20          # Parallel EventFrame fetches
ATTR_MAX_WORKERS = 5      # Parallel attribute fetches per EventFrame
CHUNK_SIZE = 1000         # Max EventFrames per API call

def load_config():
    """Load configuration from JSON file."""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

# Load sites configuration
CONFIG = load_config()
SITES_PARAM = CONFIG['sites']

# =============================================================================
# TIMEZONE UTILITIES
# =============================================================================

def to_site_local(utc_dt: datetime, site: str) -> datetime:
    """
    Convert UTC datetime to site's local timezone.

    Args:
        utc_dt: UTC datetime (must be timezone-aware)
        site: Site name for timezone lookup

    Returns:
        datetime: Site-local datetime

    Raises:
        ValueError: If datetime is naive or not UTC
        RuntimeError: If timezone data is not available
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

# =============================================================================
# AUTHENTICATION SETUP
# =============================================================================

def get_auth_for_site(site: str) -> Any:
    """
    Get authentication object for a specific site.
    
    Args:
        site: Site name
        
    Returns:
        Authentication object (tuple for Basic, HttpNegotiateAuth for Kerberos)
    """
    if SITES_PARAM[site]['AUTH'] == 'BASIC':
        username = os.getenv('PIWEBAPI_USER')
        password = os.getenv('PIWEBAPI_PASS')
        if not username or not password:
            raise ValueError(
                "BASIC auth requires PIWEBAPI_USER and PIWEBAPI_PASS environment variables"
            )
        return (username, password)
    else:
        return HttpNegotiateAuth()

# =============================================================================
# CORE FETCHING FUNCTIONS
# =============================================================================

def fetch_eventframes(site: str,
                      start_time: datetime | str,
                      end_time: datetime | str | None = None,
                      debugMode: bool = False) -> Tuple[List[Dict[str, Any]], Set[str]]:
    """
    Fetch Event Frames for a site between start and end UTC datetimes.

    Features:
    - Recursive chunking for large datasets (splits if >1000 results)
    - Filters out in-progress events (EndTime = 9999-12-31...)
    - Parallel attribute fetching with thread-safe sessions
    - Comprehensive error handling

    Args:
        site: Site name (must exist in SITES_PARAM)
        start_time: Start time (UTC string or datetime)
        end_time: End time (UTC string or datetime), defaults to now
        debugMode: Enable verbose logging

    Returns:
        Tuple of (event_data_list, unique_attribute_names_set)
    """
    if not start_time or not site:
        print("❌ Invalid parameters: start_time and site must be provided.")
        return [], set()

    if site not in SITES_PARAM:
        print(f"❌ Site '{site}' not found in configuration.")
        return [], set()

    # Parse and validate datetime inputs
    chunk_start = parse_date(start_time) if isinstance(start_time, str) else start_time
    chunk_end = parse_date(end_time) if isinstance(end_time, str) else end_time
    
    if chunk_end is None:
        chunk_end = datetime.now(timezone.utc)

    # Ensure timezone awareness
    if chunk_start.tzinfo is None:
        chunk_start = chunk_start.replace(tzinfo=timezone.utc)
    if chunk_end.tzinfo is None:
        chunk_end = chunk_end.replace(tzinfo=timezone.utc)

    if debugMode:
        print(f"🔍 Fetching EventFrames for {site}")
        print(f"📅 Time range: {chunk_start} to {chunk_end}")

    # Initialize data structures
    all_data: List[Dict[str, Any]] = []
    all_attrs: Set[str] = set()
    processing_stack = [(chunk_start, chunk_end)]

    # Setup authentication
    auth = get_auth_for_site(site)

    # Main processing loop with chunking
    with requests.Session() as list_session:
        list_session.auth = auth
        list_session.verify = False
        list_session.headers.update({'Accept': 'application/json'})

        while processing_stack:
            start, end = processing_stack.pop()
            
            if debugMode:
                duration = end - start
                print(f"🔄 Processing chunk: {start} to {end} ({duration.days} days)")

            # Fetch EventFrame list for this chunk
            eventframes = _fetch_eventframe_list(list_session, site, start, end, debugMode)
            
            if len(eventframes) >= CHUNK_SIZE:
                # Too many results, split the time range
                mid = start + (end - start) / 2
                processing_stack.append((mid, end))
                processing_stack.append((start, mid))
                
                if debugMode:
                    print(f"⚠️  {len(eventframes)} items >= {CHUNK_SIZE}; splitting range")
                continue

            if not eventframes:
                if debugMode:
                    print("ℹ️  No EventFrames found in this chunk")
                continue

            # Filter out in-progress events
            filtered_eventframes = [
                ef for ef in eventframes 
                if not (ef.get('EndTime') or '').startswith('9999-12-31')
            ]
            
            removed_count = len(eventframes) - len(filtered_eventframes)
            if debugMode and removed_count > 0:
                print(f"🧹 Filtered out {removed_count} in-progress events")

            if not filtered_eventframes:
                continue

            if debugMode:
                print(f"📊 Processing {len(filtered_eventframes)} EventFrames for attributes...")

            # Parallel attribute fetching
            chunk_data, chunk_attrs = _fetch_attributes_parallel(
                filtered_eventframes, auth, site, debugMode
            )
            
            all_data.extend(chunk_data)
            all_attrs.update(chunk_attrs)

    if debugMode:
        print(f"✅ Completed: {len(all_data)} EventFrames, {len(all_attrs)} unique attributes")

    return all_data, all_attrs

def _fetch_eventframe_list(session: requests.Session, 
                          site: str, 
                          start: datetime, 
                          end: datetime, 
                          debug: bool) -> List[Dict[str, Any]]:
    """
    Fetch the list of EventFrames for a time range.
    
    Args:
        session: Authenticated requests session
        site: Site name
        start: Start datetime (UTC)
        end: End datetime (UTC)
        debug: Debug logging flag
        
    Returns:
        List of EventFrame dictionaries
    """
    # Build API parameters
    params = {
        'startTime': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'endTime': end.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'templateName': SITES_PARAM[site]['TEMPLATE_NAME'],
    }
    
    # Construct URL
    query_string = '&'.join([f"{k}={quote_plus(str(v))}" for k, v in params.items()])
    base_url = SITES_PARAM[site]['PIWEBAPI_URL']
    db_webid = SITES_PARAM[site]['ASSET_DATABASE_WEBID']
    url = f"{base_url}/assetdatabases/{db_webid}/eventframes?{query_string}"

    try:
        if debug:
            print(f"🌐 API Call: {url}")
        
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        eventframes = data.get('Items', [])
        
        if debug:
            print(f"📦 Received {len(eventframes)} EventFrames from API")
        
        return eventframes
        
    except requests.exceptions.RequestException as e:
        if debug:
            print(f"❌ API Error: {e}")
        return []
    except Exception as e:
        if debug:
            print(f"❌ Unexpected error: {e}")
        return []

def _fetch_attributes_parallel(eventframes: List[Dict[str, Any]], 
                             auth: Any, 
                             site: str, 
                             debug: bool) -> Tuple[List[Dict[str, Any]], Set[str]]:
    """
    Fetch attributes for multiple EventFrames in parallel.
    
    Args:
        eventframes: List of EventFrame dictionaries
        auth: Authentication object
        site: Site name
        debug: Debug logging flag
        
    Returns:
        Tuple of (processed_data_list, unique_attribute_names_set)
    """
    all_data = []
    all_attrs = set()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all attribute fetch tasks
        futures = [
            executor.submit(fetch_attributes, None, ef, auth, site, debug)
            for ef in eventframes
        ]
        
        # Collect results as they complete
        for i, future in enumerate(as_completed(futures), 1):
            try:
                ef_data, ef_attrs = future.result()
                all_data.append(ef_data)
                all_attrs.update(ef_attrs)
                
                if debug and i % 10 == 0:
                    print(f"  ✓ Completed {i}/{len(eventframes)} EventFrames")
                    
            except Exception as e:
                if debug:
                    print(f"❌ Error processing EventFrame: {e}")
                continue
    
    return all_data, all_attrs

# =============================================================================
# ATTRIBUTE FETCHING
# =============================================================================

def fetch_attributes(_unused_session: Any, 
                    ef: Dict[str, Any], 
                    auth: Any, 
                    site: str, 
                    debugMode: bool = False) -> Tuple[Dict[str, Any], Set[str]]:
    """
    Fetch attributes for a single Event Frame using a dedicated session.

    Args:
        _unused_session: Ignored (kept for backwards compatibility)
        ef: EventFrame dictionary from PI Web API
        auth: Authentication object
        site: Site name for timezone conversion
        debugMode: Enable verbose logging

    Returns:
        Tuple of (eventframe_data_dict, attribute_names_set)
    """
    # Parse basic EventFrame information
    ef_data = _parse_eventframe_basic_info(ef, site)
    
    # Get attributes link
    attributes_link = ef.get('Links', {}).get('Attributes')
    if not attributes_link:
        if debugMode:
            print(f"ℹ️  EventFrame '{ef.get('Name')}' has no attributes link")
        return ef_data, set()

    attribute_names = set()
    
    # Use dedicated session for thread safety
    with requests.Session() as session:
        session.auth = auth
        session.verify = False
        session.headers.update({'Accept': 'application/json'})
        
        try:
            # Fetch attribute list
            attr_response = session.get(attributes_link, timeout=30)
            attr_response.raise_for_status()
            attr_data = attr_response.json()
            
            attr_items = attr_data.get('Items', [])
            
            if not attr_items:
                if debugMode:
                    print(f"ℹ️  No attributes found for '{ef.get('Name')}'")
                return ef_data, attribute_names

            # Fetch attribute values in parallel
            ef_attrs = _fetch_attribute_values_parallel(attr_items, session)
            
            # Merge attributes into EventFrame data
            ef_data.update(ef_attrs)
            attribute_names.update(ef_attrs.keys())
            
            if debugMode:
                print(f"✅ Fetched {len(attribute_names)} attributes for '{ef.get('Name')}'")
                
        except Exception as e:
            if debugMode:
                print(f"❌ Error fetching attributes for '{ef.get('Name')}': {e}")

    return ef_data, attribute_names

def _parse_eventframe_basic_info(ef: Dict[str, Any], site: str) -> Dict[str, Any]:
    """
    Parse basic EventFrame information and convert times to local timezone.
    
    Args:
        ef: EventFrame dictionary
        site: Site name for timezone conversion
        
    Returns:
        Dictionary with basic EventFrame information
    """
    start_time_utc = ef.get('StartTime')
    end_time_utc = ef.get('EndTime')

    # Convert to local time
    start_time_local = _convert_to_local_time(start_time_utc, site)
    end_time_local = _convert_to_local_time(end_time_utc, site)

    return {
        'Event Frame Name': ef.get('Name'),
        'Start Time': start_time_local,
        'Start Time UTC': start_time_utc,
        'End Time': end_time_local,
        'End Time UTC': end_time_utc,
        'Template Name': ef.get('TemplateName'),
        'WebId': ef.get('WebId'),
        'Id': ef.get('Id')
    }

def _convert_to_local_time(utc_time_str: str, site: str) -> str:
    """Convert UTC time string to site local time string."""
    if not utc_time_str or not site:
        return ''
    
    try:
        utc_dt = parse_date(utc_time_str)
        local_dt = to_site_local(utc_dt, site)
        return local_dt.strftime('%Y-%m-%dT%H:%M:%S')
    except Exception:
        return ''

def _fetch_attribute_values_parallel(attr_items: List[Dict[str, Any]], 
                                   session: requests.Session) -> Dict[str, Any]:
    """
    Fetch attribute values in parallel using thread pool.
    
    Args:
        attr_items: List of attribute dictionaries
        session: Authenticated requests session
        
    Returns:
        Dictionary mapping attribute names to values
    """
    def fetch_single_attribute_value(attr: Dict[str, Any]) -> Tuple[str, Any]:
        """Fetch value for a single attribute."""
        name = attr.get('Name')
        value_link = attr.get('Links', {}).get('Value')
        
        try:
            if value_link:
                # Fetch value from separate endpoint
                val_response = session.get(value_link, timeout=10)
                val_response.raise_for_status()
                val_data = val_response.json()
                actual_value = val_data.get('Value')
            else:
                # Use embedded value
                actual_value = attr.get('Value')

            # Serialize complex objects
            if isinstance(actual_value, (dict, list)):
                value_to_store = json.dumps(actual_value)
            else:
                value_to_store = actual_value

            return name, value_to_store
            
        except Exception:
            return name, ""

    # Use thread pool for parallel attribute value fetching
    attribute_values = {}
    
    with ThreadPoolExecutor(max_workers=ATTR_MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_single_attribute_value, attr) for attr in attr_items]
        
        for future in as_completed(futures):
            try:
                name, value = future.result()
                if name:
                    attribute_values[name] = value
            except Exception:
                continue  # Skip failed attributes
    
    return attribute_values

# =============================================================================
# TESTING AND DEMO
# =============================================================================

def demo_fetch():
    """Demo function showing basic usage."""
    print("🧪 PI Web API EventFrame Fetcher Demo")
    print("=" * 50)
    
    # Test timezone conversion
    print("\n📍 Timezone Conversion Test:")
    test_utc = datetime(2025, 6, 30, 19, 56, 40, 23000, tzinfo=timezone.utc)
    
    for site in ['EGYPT', 'TRINIDAD', 'CHILE']:
        if site in SITES_PARAM:
            try:
                local_time = to_site_local(test_utc, site)
                print(f"  {site}: {test_utc} UTC → {local_time}")
            except Exception as e:
                print(f"  {site}: Error - {e}")
    
    print("\n💡 To fetch actual data, uncomment and modify:")
    print("# data, attrs = fetch_eventframes('SITE_NAME', ")
    print("#                                '2025-01-01T00:00:00Z', ")
    print("#                                '2025-01-31T23:59:59Z', ")
    print("#                                debugMode=True)")
    print("# print(f'Fetched {len(data)} event frames with {len(attrs)} unique attributes')")

if __name__ == "__main__":
    demo_fetch()
