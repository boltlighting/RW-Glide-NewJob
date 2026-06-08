#!/usr/bin/env python3
"""
RentalWorks → Glide New Job Sync

Uses:
- RentalWorks API for recent orders
- Glide function API:
  - queryTables for reading rows
  - mutateTables for adding/updating rows

Required .env:
  RW_BASE_URL=https://boltlighting.rentalworks.cloud
  RW_USERNAME=your_rw_username
  RW_PASSWORD=your_rw_password

  GLIDE_API_KEY=your_glide_token
  GLIDE_API_BASE=https://api.glideapp.io/api/function
  GLIDE_APP_ID=iL4p0gGcmvkoAQ8oefyM
  GLIDE_TABLE_NAME=native-table-1L4wPB53vJM273qy7HAu

Recommended while testing:
  DRY_RUN=true
  PAGE_LIMIT=5
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import requests


# ------------------------- Utilities -------------------------

def log(msg: str, level: str = "INFO"):
    print(f"[rw→glide][{level}] {msg}", flush=True)


def load_env(path: str = ".env"):
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()

            if not s or s.startswith("#") or "=" not in s:
                continue

            k, v = s.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def env(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    val = os.getenv(name, default)

    if required and not val:
        raise RuntimeError(f"Missing required env var: {name}")

    return val


def env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)

    if val is None:
        return default

    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def with_retries(fn: Callable[[], requests.Response], *, attempts=3, base_delay=0.8) -> requests.Response:
    last = None

    for i in range(attempts):
        try:
            resp = fn()

            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"{resp.status_code}", response=resp)

            return resp

        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            last = e

            if i < attempts - 1:
                time.sleep(base_delay * (2 ** i))
            else:
                raise

    raise last


# ------------------------- Local state -------------------------

class State:
    def __init__(self, path: str):
        self.path = path
        self.data: Dict[str, Any] = {
            "processed": [],
            "order_locations": {},
            "order_row_ids": {},
        }

        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)

                if isinstance(loaded, dict):
                    self.data.update(loaded)

            except Exception:
                log("State file corrupt; starting fresh", "WARN")

        self.data.setdefault("processed", [])
        self.data.setdefault("order_locations", {})
        self.data.setdefault("order_row_ids", {})

    def save(self):
        tmp = self.path + ".tmp"

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, sort_keys=True)

        os.replace(tmp, self.path)

    def set_row_id(self, order_id: int | str, row_id: str):
        self.data["order_row_ids"][str(order_id)] = row_id
        self.mark_processed(order_id)
        self.save()

    def get_row_id(self, order_id: int | str) -> Optional[str]:
        return self.data["order_row_ids"].get(str(order_id))

    def mark_processed(self, order_id: int | str):
        sid = str(order_id)
        processed = [str(x) for x in self.data.get("processed", [])]

        if sid not in processed:
            self.data["processed"].append(sid)

    def set_location(self, order_id: int | str, location: str):
        self.data["order_locations"][str(order_id)] = location
        self.save()


# ------------------------- RentalWorks client -------------------------

class RWClient:
    def __init__(self, base_url: str, username: str, password: str, tenant: Optional[str] = None):
        self.base = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.tenant = tenant
        self.sess = requests.Session()
        self.sess.headers.update({"Accept": "application/json"})
        self.token: Optional[str] = None

    def login(self):
        url = f"{self.base}/api/v1/jwt"

        payload = {
            "UserName": self.username,
            "Password": self.password,
        }

        if self.tenant:
            payload["Tenant"] = self.tenant

        resp = with_retries(lambda: self.sess.post(url, json=payload, timeout=30))
        resp.raise_for_status()

        data = resp.json() if resp.content else {}

        self.token = (
            data.get("access_token")
            or data.get("token")
            or data.get("Token")
            or data.get("AccessToken")
        )

        if not self.token:
            raise RuntimeError(f"JWT token missing in RW response. Response keys: {list(data.keys())}")

        self.sess.headers.update({"Authorization": f"Bearer {self.token}"})

    def fetch_recent_orders(self, limit: int = 50) -> List[Dict[str, Any]]:
        urls_to_try = [
            f"{self.base}/api/v1/order",
            f"{self.base}/api/v1/orders",
        ]

        last_error = None

        for url in urls_to_try:
            try:
                params = {
                    "pagesize": limit,
                    "sort": "OrderNumber desc",
                }

                resp = with_retries(lambda: self.sess.get(url, params=params, timeout=45))
                resp.raise_for_status()

                data = resp.json() if resp.content else {}

                if isinstance(data, dict):
                    if isinstance(data.get("Items"), list):
                        return data["Items"]

                    if isinstance(data.get("items"), list):
                        return data["items"]

                    if isinstance(data.get("Data"), list):
                        return data["Data"]

                if isinstance(data, list):
                    return data

            except Exception as e:
                last_error = e
                continue

        raise RuntimeError(f"Could not fetch RW orders. Last error: {last_error}")


# ------------------------- Glide function API client -------------------------

class GlideClient:
    def __init__(self, api_base: str, app_id: str, table_name: str, api_key: str, dry_run: bool = True):
        self.base = api_base.rstrip("/")
        self.app_id = app_id
        self.table_name = table_name
        self.dry_run = dry_run

        self.sess = requests.Session()
        self.sess.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def query_url(self) -> str:
        return f"{self.base}/queryTables"

    def mutate_url(self) -> str:
        return f"{self.base}/mutateTables"

    def list_rows(self) -> List[Dict[str, Any]]:
        body = {
            "appID": self.app_id,
            "queries": [
                {
                    "tableName": self.table_name,
                    "utc": True,
                }
            ],
        }

        resp = with_retries(lambda: self.sess.post(self.query_url(), json=body, timeout=45))
        resp.raise_for_status()

        data = resp.json() if resp.content else {}

        if isinstance(data, dict):
            if isinstance(data.get("data"), list):
                first = data["data"][0] if data["data"] else {}

                if isinstance(first, dict):
                    if isinstance(first.get("rows"), list):
                        return first["rows"]

                    if isinstance(first.get("data"), list):
                        return first["data"]

                if isinstance(first, list):
                    return first

            if isinstance(data.get("rows"), list):
                return data["rows"]

            if isinstance(data.get("result"), list):
                return data["result"]

        if isinstance(data, list):
            first = data[0] if data else {}

            if isinstance(first, dict) and isinstance(first.get("rows"), list):
                return first["rows"]

            return data

        log(f"Could not understand Glide query response: {json.dumps(data, indent=2)}", "WARN")
        return []

    def get_row_id(self, row: Dict[str, Any]) -> Optional[str]:
        for key in ("$rowID", "rowID", "rowId", "id", "ID"):
            if row.get(key):
                return str(row[key])

        return None

    def find_row_by_order_id(self, order_id: int | str) -> Optional[Dict[str, Any]]:
        sid = str(order_id).strip()

        for row in self.list_rows():
            value = row.get("grcGy")

            if str(value or "").strip() == sid:
                return row

        return None

    def mutate(self, mutation: Dict[str, Any]) -> Any:
        body = {
            "appID": self.app_id,
            "mutations": [mutation],
        }

        if self.dry_run:
            log(f"DRY_RUN Glide mutation: {json.dumps(body, indent=2)}")
            return {"dryRun": True}

        resp = with_retries(lambda: self.sess.post(self.mutate_url(), json=body, timeout=45))
        resp.raise_for_status()

        return resp.json() if resp.content else {}

    def extract_row_id_from_mutation_result(self, result: Any) -> Optional[str]:
        if isinstance(result, list):
            first = result[0] if result else {}

            if isinstance(first, dict):
                for key in ("rowID", "rowId", "id", "ID", "$rowID"):
                    if first.get(key):
                        return str(first[key])

            return None

        if isinstance(result, dict):
            for key in ("rowID", "rowId", "id", "ID", "$rowID"):
                if result.get(key):
                    return str(result[key])

            for key in ("data", "result", "results"):
                value = result.get(key)

                if isinstance(value, list) and value:
                    first = value[0]

                    if isinstance(first, dict):
                        for row_key in ("rowID", "rowId", "id", "ID", "$rowID"):
                            if first.get(row_key):
                                return str(first[row_key])

                if isinstance(value, dict):
                    for row_key in ("rowID", "rowId", "id", "ID", "$rowID"):
                        if value.get(row_key):
                            return str(value[row_key])

        return None

    def insert_row(self, column_values: Dict[str, Any]) -> Optional[str]:
        mutation = {
            "kind": "add-row-to-table",
            "tableName": self.table_name,
            "columnValues": column_values,
        }

        result = self.mutate(mutation)
        row_id = self.extract_row_id_from_mutation_result(result)

        if row_id:
            return row_id

        if not self.dry_run:
            log(f"Glide insert response did not include an obvious row ID: {result}", "WARN")

        return None

    def update_row(self, row_id: str, column_values: Dict[str, Any]):
        mutation = {
            "kind": "set-columns-in-row",
            "tableName": self.table_name,
            "rowID": row_id,
            "columnValues": column_values,
        }

        self.mutate(mutation)


# ------------------------- RentalWorks field helpers -------------------------

CONFIRM_STRINGS = {"CONFIRMED", "CONFIRM"}
CLOSED_STRINGS = {"CLOSED", "CANCELLED", "CANCELED", "COMPLETE", "COMPLETED"}


def get_order_id(o: Dict[str, Any]) -> Optional[int]:
    for k in ("OrderNumber", "OrderId", "OrderID", "Id", "ID"):
        if k in o and o.get(k) is not None:
            try:
                return int(o[k])
            except Exception:
                pass

    return None


def get_status_text(o: Dict[str, Any]) -> str:
    for k in ("StatusName", "Status", "OrderStatus", "OrderStatusName"):
        if o.get(k):
            return str(o[k]).strip().upper()

    sid = o.get("StatusId") or o.get("StatusID") or o.get("OrderStatusId")

    if sid is not None:
        try:
            if int(sid) == 3:
                return "CONFIRMED"
        except Exception:
            pass

    return ""


def is_confirmed(o: Dict[str, Any]) -> bool:
    st = get_status_text(o)
    return bool(st) and (st in CONFIRM_STRINGS or any(s in st for s in CONFIRM_STRINGS))


def is_closed_like(o: Dict[str, Any]) -> bool:
    st = get_status_text(o)
    return bool(st) and any(s in st for s in CLOSED_STRINGS)


def get_location(o: Dict[str, Any]) -> str:
    # Correct RW field for truck/shed/workflow location:
    # SHED 2, SHED 3, SHED 4, 1 TON, 3 TON, WAREHOUSE, etc.
    for k in (
        "OrderLocation",
        "Location",
    ):
        if o.get(k):
            return str(o[k]).strip()

    # Fallbacks only if OrderLocation/Location are empty
    for k in (
        "Warehouse",
        "OfficeLocation",
        "Office",
        "OfficeName",
        "DepartmentLocation",
    ):
        if o.get(k):
            return str(o[k]).strip()

    return ""


def get_customer_name(o: Dict[str, Any]) -> str:
    for k in ("CustomerName", "ClientName", "BillToName", "IssuedToName"):
        if o.get(k):
            return str(o[k]).strip()

    customer = o.get("Customer")

    if isinstance(customer, dict):
        for k in ("Name", "CustomerName", "CompanyName"):
            if customer.get(k):
                return str(customer[k]).strip()

    return ""


def get_job_name(o: Dict[str, Any]) -> str:
    for k in ("JobName", "Name", "Description", "OrderDescription", "ProjectName"):
        if o.get(k):
            return str(o[k]).strip()

    return ""


def build_glide_column_values(o: Dict[str, Any], archive: bool = False) -> Dict[str, Any]:
    """
    Glide column IDs:
      GARI1 = Initial Data / Project Name
      grcGy = Initial Data / Order ID
      CoRZS = Location
      LFpCG = Return / Archive
    """

    return {
        "GARI1": get_job_name(o),
        "grcGy": str(get_order_id(o) or ""),
        "CoRZS": get_location(o),
        "LFpCG": bool(archive),
    }


# ------------------------- Reconcile logic -------------------------

def reconcile_order(o: Dict[str, Any], st: State, glide: GlideClient):
    oid = get_order_id(o)

    if not oid:
        log("Skip: RW order without Order ID", "WARN")
        return

    status = get_status_text(o)
    location = get_location(o)

    existing = glide.find_row_by_order_id(oid)

    if is_closed_like(o):
        if not existing:
            log(f"Skip archive {oid}: not found in Glide")
            return

        row_id = glide.get_row_id(existing)

        if not row_id:
            log(f"Skip archive {oid}: Glide row found but no rowID present", "WARN")
            return

        glide.update_row(row_id, {"LFpCG": True})
        st.set_row_id(oid, row_id)
        st.set_location(oid, location)
        log(f"Archived {oid}")
        return

    if not is_confirmed(o):
        log(f"Skip {oid}: non-confirmed ({status or 'unknown'})")
        return

    column_values = build_glide_column_values(o, archive=False)

    if existing:
        row_id = glide.get_row_id(existing)

        if not row_id:
            log(f"Skip update {oid}: Glide row found but no rowID present", "WARN")
            return

        existing_location = str(existing.get("CoRZS") or "")

        if existing_location != location:
            log(f"Location changed for {oid}: {existing_location!r} -> {location!r}")
            glide.update_row(row_id, column_values)
            log(f"Updated {oid}")
        else:
            log(f"Skip insert {oid}: already exists in Glide")

        st.set_row_id(oid, row_id)
        st.set_location(oid, location)
        return

    new_row_id = glide.insert_row(column_values)

    if new_row_id:
        st.set_row_id(oid, new_row_id)

    st.set_location(oid, location)
    log(f"Inserted {oid}")


# ------------------------- Runner -------------------------

def run_once():
    load_env()

    rw_base = env("RW_BASE_URL", required=True)
    rw_user = env("RW_USERNAME", required=True)
    rw_pass = env("RW_PASSWORD", required=True)
    rw_tenant = env("RW_TENANT")

    glide_key = env("GLIDE_API_KEY", required=True)
    glide_base = env("GLIDE_API_BASE") or "https://api.glideapp.io/api/function"
    glide_app_id = env("GLIDE_APP_ID", required=True)
    glide_table = env("GLIDE_TABLE_NAME") or env("GLIDE_TABLE")

    if not glide_table:
        raise RuntimeError("Missing required env var: GLIDE_TABLE_NAME or GLIDE_TABLE")

    page_limit = int(env("PAGE_LIMIT") or "50")
    state_path = env("STATE_PATH") or "./rw_glide_state.json"
    dry_run = env_bool("DRY_RUN", default=True)

    parser = argparse.ArgumentParser(description="RentalWorks → Glide New Job Sync")
    parser.add_argument("--live", action="store_true", help="Override DRY_RUN and actually write to Glide.")
    args = parser.parse_args()

    if args.live:
        dry_run = False

    log(f"DRY_RUN={dry_run}")
    log(f"Glide app={glide_app_id}")
    log(f"Glide table={glide_table}")

    st = State(state_path)

    rw = RWClient(
        base_url=rw_base,
        username=rw_user,
        password=rw_pass,
        tenant=rw_tenant,
    )

    rw.login()

    glide = GlideClient(
        api_base=glide_base,
        app_id=glide_app_id,
        table_name=glide_table,
        api_key=glide_key,
        dry_run=dry_run,
    )

    log("Polling RW for recent orders")

    orders = rw.fetch_recent_orders(limit=page_limit)

    if not orders:
        log("No orders returned from RW")
        return

    log(f"Fetched {len(orders)} orders from RW")

    for o in orders:
        try:
            reconcile_order(o, st, glide)

        except requests.HTTPError as http_err:
            oid = get_order_id(o)
            code = getattr(http_err.response, "status_code", "unknown")

            try:
                detail = http_err.response.text
            except Exception:
                detail = ""

            log(f"HTTP {code} for order {oid}: {http_err} {detail}", "WARN")

        except Exception as e:
            log(f"Unexpected error for order {get_order_id(o)}: {e}", "WARN")
            traceback.print_exc()


if __name__ == "__main__":
    try:
        run_once()

    except Exception as e:
        log(f"Fatal error: {e}", "ERROR")
        traceback.print_exc()
        sys.exit(1)