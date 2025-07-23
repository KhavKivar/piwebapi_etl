"""
PI Web API Event Frame fetcher (thread-safe attribute retrieval).

Quick-fix version: each attribute fetch runs in its own requests.Session so that
we never share a Session across worker threads (requests.Session is *not* thread-safe).

Other small cleanups:
- Correct EndTime filter key (was 'End Time').
- Defensive parsing of PI value payloads.
- Reduced duplicated imports.
- Optional debug logging.

Drop-in compatible: fetch_attributes() keeps the old positional signature
(session, ef, AUTH, site, debugMode=False) but the `session` argument is ignored.

Test block at bottom shows basic usage.
"""

from __future__ import annotations

import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests
import urllib3
from urllib.parse import quote_plus
from dateutil.parser import parse as parse_date
from dotenv import load_dotenv
from requests_negotiate_sspi import HttpNegotiateAuth
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Optional Excel helpers kept (not used in core fetch but imported by caller code)
from openpyxl.utils import get_column_letter  # noqa: F401
from openpyxl.styles import Alignment  # noqa: F401

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # self-signed certs

MAX_WORKERS = 20          # parallel EventFrame attribute fetches
ATTR_MAX_WORKERS = 5      # parallel attribute value fetches per EventFrame

# Caller must provide SITES_PARAM mapping in site_param.py
from site_param import SITES_PARAM  # type: ignore  # user-supplied module

# ---------------------------------------------------------------------------
# Time conversion
# ---------------------------------------------------------------------------

def to_site_local(utc_dt: datetime, site: str) -> datetime:
    """Convert an aware UTC datetime to the site's local tz.

    Parameters
    ----------
    utc_dt : datetime
        Must be timezone-aware and in UTC.
    site : str
        Key into SITES_PARAM.
    """
    if utc_dt.tzinfo is None:
        raise ValueError("utc_dt is naive; pass an aware UTC datetime (tzinfo=timezone.utc).")
    if utc_dt.tzinfo.utcoffset(utc_dt) != timezone.utc.utcoffset(utc_dt):
        raise ValueError("utc_dt is not in UTC; make sure to pass a UTC datetime.")

    tz_name = SITES_PARAM[site]['TIMEZONE']
    try:
        return utc_dt.astimezone(ZoneInfo(tz_name))
    except ZoneInfoNotFoundError as e:  # pragma: no cover - environment specific
        raise RuntimeError(
            f"Time zone data not found for '{tz_name}'. Install tzdata: pip install tzdata"
        ) from e


# ---------------------------------------------------------------------------
# Core fetch
# ---------------------------------------------------------------------------

def fetch_eventframes(site: str,
                      start_time: datetime | str,
                      end_time: datetime | str | None = None,
                      debugMode: bool = False) -> Tuple[List[Dict[str, Any]], Set[str]]:
    """Fetch Event Frames for a site between start & end UTC datetimes.

    Splits very large ranges recursively until fewer than 1000 EFs are returned.
    Filters out in-progress frames (EndTime = 9999-12-31...).
    Fetches attributes for each EF in parallel (MAX_WORKERS), using *one Session per thread*.
    """
    if start_time is None or site is None:
        print("❌ Invalid parameters: start_time and site must be provided.")
        return [], set()

    # Parse incoming datetimes ------------------------------------------------
    chunk_start = parse_date(start_time) if isinstance(start_time, str) else start_time
    if end_time is None:
        chunk_end = datetime.now(timezone.utc)
    else:
        chunk_end = parse_date(end_time) if isinstance(end_time, str) else end_time

    chunk_size = 1000
    all_data: List[Dict[str, Any]] = []
    all_attrs: Set[str] = set()
    stack: List[Tuple[datetime, datetime]] = [(chunk_start, chunk_end)]

    # Auth --------------------------------------------------------------------
    if SITES_PARAM[site]['AUTH'] == 'BASIC':
        AUTH: Any = (os.getenv('PIWEBAPI_USER'), os.getenv('PIWEBAPI_PASS'))
    else:
        AUTH = HttpNegotiateAuth()

    # Reusable session for *event-frame list* calls only (single-threaded use)
    with requests.Session() as list_session:
        list_session.auth = AUTH

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

            try:
                resp = list_session.get(url, verify=False, headers={'Accept': 'application/json'})
                if debugMode:
                    print(f"\n🔗 Fetching Event Frames from: {url}\n    Range: {start} to {end}")
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:  # pragma: no cover - network error path
                if debugMode:
                    print(f"❌ Error fetching Event Frames: {e}")
                continue

            items = data.get('Items', [])
            if len(items) >= chunk_size:
                # Too many items, split the range in half and push both halves to stack
                mid = start + (end - start) / 2
                stack.append((mid, end))
                stack.append((start, mid))
                if debugMode:
                    print(f"⚠️  {len(items)} items >= chunk_size {chunk_size}; splitting range")
                continue

            if not items:
                continue

            # Remove in-progress frames (EndTime far future / open)
            # NOTE: PI returns 'EndTime' (camel-case). Old code used 'End Time' -> always empty.
            items = [ef for ef in items if not (ef.get('EndTime') or '').startswith('9999-12-31')]

            if debugMode:
                print(f"🔍 {len(items)} Event Frames after filtering. Fetching attributes…")

            # Parallel attribute fetches (thread-safe: one Session per thread)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(fetch_attributes, None, ef, AUTH, site, debugMode)
                           for ef in items]
                for f in as_completed(futures):
                    row_data, attr_names = f.result()
                    all_data.append(row_data)
                    all_attrs.update(attr_names)

    return all_data, all_attrs


# ---------------------------------------------------------------------------
# Attribute fetch (thread-safe)
# ---------------------------------------------------------------------------

def _extract_value_payload(val_data: Any) -> Any:
    """Extract the actual value from a PI Web API value payload.

    Common payload shapes:
    {"Value": 123, ...}
    {"Value": {"Value": 123, ...}, ...}
    """
    if isinstance(val_data, dict):
        if 'Value' in val_data:
            v = val_data['Value']
            if isinstance(v, dict) and 'Value' in v:
                return v['Value']
            return v
    return val_data


def fetch_attributes(_unused_session, ef: Dict[str, Any], AUTH: Any, site: str, debugMode: bool = False):
    """Fetch attributes for a single Event Frame.

    NOTE: `_unused_session` param kept for backward compatibility; ignored.
    A *new* Session is created per call to avoid thread-safety issues.
    """
    # Parse UTC times ---------------------------------------------------------
    start_time_utc = ef.get('StartTime')
    end_time_utc = ef.get('EndTime')

    # Convert to local time using to_site_local --------------------------------
    if site and start_time_utc:
        try:
            start_time_local = to_site_local(parse_date(start_time_utc), site).strftime('%Y-%m-%dT%H:%M:%S')
        except Exception:  # pragma: no cover - parse/tz error path
            start_time_local = ''
    else:
        start_time_local = ''

    if site and end_time_utc:
        try:
            end_time_local = to_site_local(parse_date(end_time_utc), site).strftime('%Y-%m-%dT%H:%M:%S')
        except Exception:  # pragma: no cover
            end_time_local = ''
    else:
        end_time_local = ''

    ef_row_data: Dict[str, Any] = {
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

    attribute_names: Set[str] = set()

    # Open a fresh Session for this EF ---------------------------------------
    with requests.Session() as sess:
        sess.auth = AUTH
        try:
            attr_resp = sess.get(attributes_link, verify=False, headers={'Accept': 'application/json'})
            attr_resp.raise_for_status()
            attr_data = attr_resp.json()
        except Exception as e:  # pragma: no cover - network error path
            if debugMode:
                print(f"❌ Error fetching attributes for EF '{ef.get('Name')}': {e}")
                print(f"   URL: {attributes_link}")
            return ef_row_data, attribute_names

        attr_items = attr_data.get('Items', []) if attr_data else []

        # Fetch attribute values ------------------------------------------------
        def fetch_value(attr: Dict[str, Any]):
            name = attr.get('Name')
            value_link = attr.get('Links', {}).get('Value')
            try:
                if value_link:
                    val_resp = sess.get(value_link, verify=False, headers={'Accept': 'application/json'})
                    val_resp.raise_for_status()
                    val_data = val_resp.json()
                    actual_value = _extract_value_payload(val_data)
                else:
                    actual_value = attr.get('Value')

                if isinstance(actual_value, (dict, list)):
                    value_to_store = json.dumps(actual_value)
                else:
                    value_to_store = actual_value

            except Exception:  # pragma: no cover
                value_to_store = ""

            return name, value_to_store

        # Parallel per-EF value fetch (can be reduced to serial if desired)
        if attr_items:
            with ThreadPoolExecutor(max_workers=ATTR_MAX_WORKERS) as value_executor:
                futures = [value_executor.submit(fetch_value, attr) for attr in attr_items]
                for f in as_completed(futures):
                    key, val = f.result()
                    if key is not None:
                        ef_row_data[key] = val
                        attribute_names.add(key)

        if debugMode:
            print(f"✅ Successfully fetched {len(attribute_names)} attributes for EF: '{ef.get('Name')}'")

    return ef_row_data, attribute_names


# ---------------------------------------------------------------------------
# Demo / Manual test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Quick timezone sanity check
    print("Timezone test:")
    print(to_site_local(datetime(2025, 6, 30, 19, 56, 40, 23000, tzinfo=timezone.utc), 'EGYPT'))

    # Minimal smoke test (adjust site & dates to match your environment)
    # Uncomment to run:
    # data, attrs = fetch_eventframes('EGYPT', '2025-06-01T00:00:00Z', end_time='2025-06-30T23:59:59Z', debugMode=True)
    # print(f"Fetched {len(data)} event frames; {len(attrs)} unique attribute names")
