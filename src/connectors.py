"""
Unified connector registry for Relay
Consolidates all source fetching into a single module.
"""

import logging
import random
import string
import uuid
from datetime import datetime, timedelta
from io import StringIO
from typing import Dict, Iterator

import pandas as pd
import requests
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


class ConnectorRegistry:
    """Central registry for all data source connectors."""

    _HANDLERS = {}
    _STREAMING_HANDLERS = {}

    @classmethod
    def register(cls, source_type: str, *, streaming: bool = False):
        """Decorator to register a fetch handler."""
        def decorator(fn):
            if streaming:
                cls._STREAMING_HANDLERS[source_type] = fn
            else:
                cls._HANDLERS[source_type] = fn
            return fn
        return decorator

    @classmethod
    def _resolve_connection(cls, source: Dict) -> Dict:
        """If source references a named connection, look it up, decrypt, and merge credentials."""
        connection_name = source.get("connection")
        if not connection_name:
            return source

        from .storage import Storage

        storage = Storage()
        conn = storage.get_connection_by_name(connection_name, include_credentials=True)
        if not conn:
            raise ValueError(f"Connection '{connection_name}' not found")

        if conn["type"] != source["type"]:
            raise ValueError(
                f"Connection type mismatch: connection '{connection_name}' is type '{conn['type']}' "
                f"but source specifies type '{source['type']}'"
            )

        merged = dict(source)
        merged.pop("connection")
        # Connection credentials are the base; source fields override
        for key, value in conn["credentials"].items():
            if key not in merged:
                merged[key] = value
        return merged

    @classmethod
    def fetch_source(cls, source: Dict) -> pd.DataFrame:
        """Fetch data from any registered source type."""
        source = cls._resolve_connection(source)
        source_type = source["type"]
        handler = cls._HANDLERS.get(source_type)
        if not handler:
            raise ValueError(f"Unsupported source type: {source_type}")
        return handler(source)

    @classmethod
    def fetch_source_streaming(cls, source: Dict, chunk_size: int = 10000) -> Iterator[pd.DataFrame]:
        """Fetch data from any registered streaming source type."""
        source = cls._resolve_connection(source)
        source_type = source["type"]
        handler = cls._STREAMING_HANDLERS.get(source_type)
        if handler:
            yield from handler(source, chunk_size)
        else:
            # Fall back to non-streaming fetch yielded as single chunk
            df = cls.fetch_source(source)
            yield df

    @classmethod
    def supported_types(cls) -> list:
        return sorted(set(list(cls._HANDLERS.keys()) + list(cls._STREAMING_HANDLERS.keys())))

    @classmethod
    def test_connection(cls, conn_type: str, credentials: Dict) -> Dict:
        """Test connectivity for a given connection type and credentials."""
        try:
            if conn_type == "mysql":
                conn_str = (
                    f"mysql+pymysql://{credentials['username']}:{credentials['password']}"
                    f"@{credentials['host']}:{credentials.get('port', 3306)}/{credentials['database']}"
                )
                engine = create_engine(conn_str)
                with engine.connect() as conn:
                    conn.execute(pd.io.sql.text("SELECT 1"))
                engine.dispose()
                return {"status": "success", "message": "Connected to MySQL successfully"}

            elif conn_type == "postgres":
                conn_str = (
                    f"postgresql://{credentials['username']}:{credentials['password']}"
                    f"@{credentials['host']}:{credentials.get('port', 5432)}/{credentials['database']}"
                )
                engine = create_engine(conn_str)
                with engine.connect() as conn:
                    conn.execute(pd.io.sql.text("SELECT 1"))
                engine.dispose()
                return {"status": "success", "message": "Connected to PostgreSQL successfully"}

            elif conn_type == "salesforce":
                from simple_salesforce import Salesforce

                Salesforce(
                    username=credentials["username"],
                    password=credentials["password"],
                    security_token=credentials.get("security_token", ""),
                    domain=credentials.get("domain", "login"),
                )
                return {"status": "success", "message": "Authenticated with Salesforce successfully"}

            elif conn_type == "rest_api":
                url = credentials.get("base_url") or credentials.get("url", "")
                if url:
                    resp = requests.get(url, timeout=10)
                    return {"status": "success", "message": f"Reachable (HTTP {resp.status_code})"}
                return {"status": "success", "message": "Credentials stored (no base_url to ping)"}

            else:
                return {"status": "success", "message": f"Credentials stored for {conn_type} (no live test available)"}

        except Exception as e:
            return {"status": "failed", "message": str(e)}


# ---------------------------------------------------------------------------
# CSV URL
# ---------------------------------------------------------------------------

@ConnectorRegistry.register("csv_url")
def _fetch_csv(source: Dict) -> pd.DataFrame:
    response = requests.get(source["url"], timeout=30)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


@ConnectorRegistry.register("csv_url", streaming=True)
def _fetch_csv_streaming(source: Dict, chunk_size: int) -> Iterator[pd.DataFrame]:
    response = requests.get(source["url"], timeout=30)
    response.raise_for_status()
    for chunk in pd.read_csv(StringIO(response.text), chunksize=chunk_size):
        yield chunk


# ---------------------------------------------------------------------------
# JSON URL
# ---------------------------------------------------------------------------

@ConnectorRegistry.register("json_url")
def _fetch_json(source: Dict) -> pd.DataFrame:
    response = requests.get(source["url"], timeout=30)
    response.raise_for_status()
    return pd.read_json(StringIO(response.text))


# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------

def _parse_rest_response(data) -> pd.DataFrame:
    """Common helper: turn a JSON response into a DataFrame."""
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        for key in ("data", "results", "items", "records"):
            if key in data and isinstance(data[key], list):
                return pd.DataFrame(data[key])
        return pd.DataFrame([data])
    raise ValueError(f"Unsupported response type: {type(data)}")


@ConnectorRegistry.register("rest_api")
def _fetch_rest_api(source: Dict) -> pd.DataFrame:
    url = source["url"]
    method = source.get("method", "GET")
    headers = dict(source.get("headers", {}))
    auth_config = source.get("auth")

    request_auth = None
    if auth_config:
        if auth_config.get("type") == "bearer":
            headers["Authorization"] = f"Bearer {auth_config['token']}"
        elif auth_config.get("type") == "basic":
            from requests.auth import HTTPBasicAuth
            request_auth = HTTPBasicAuth(auth_config["username"], auth_config["password"])

    response = requests.request(
        method, url, headers=headers, params=source.get("params", {}),
        auth=request_auth, timeout=30,
    )
    response.raise_for_status()
    return _parse_rest_response(response.json())


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------

@ConnectorRegistry.register("mysql")
def _fetch_mysql(source: Dict) -> pd.DataFrame:
    conn_str = (
        f"mysql+pymysql://{source['username']}:{source['password']}"
        f"@{source['host']}:{source.get('port', 3306)}/{source['database']}"
    )
    engine = create_engine(conn_str)
    query = source.get("query", f"SELECT * FROM {source.get('table', 'table')}")
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df


@ConnectorRegistry.register("mysql", streaming=True)
def _fetch_mysql_streaming(source: Dict, chunk_size: int) -> Iterator[pd.DataFrame]:
    conn_str = (
        f"mysql+pymysql://{source['username']}:{source['password']}"
        f"@{source['host']}:{source.get('port', 3306)}/{source['database']}"
    )
    engine = create_engine(conn_str)
    query = source.get("query", f"SELECT * FROM {source.get('table', 'table')}")
    for chunk in pd.read_sql(query, engine, chunksize=chunk_size):
        yield chunk
    engine.dispose()


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------

@ConnectorRegistry.register("postgres")
def _fetch_postgres(source: Dict) -> pd.DataFrame:
    conn_str = (
        f"postgresql://{source['username']}:{source['password']}"
        f"@{source['host']}:{source.get('port', 5432)}/{source['database']}"
    )
    engine = create_engine(conn_str)
    query = source.get("query", f"SELECT * FROM {source.get('table', 'table')}")
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df


@ConnectorRegistry.register("postgres", streaming=True)
def _fetch_postgres_streaming(source: Dict, chunk_size: int) -> Iterator[pd.DataFrame]:
    conn_str = (
        f"postgresql://{source['username']}:{source['password']}"
        f"@{source['host']}:{source.get('port', 5432)}/{source['database']}"
    )
    engine = create_engine(conn_str)
    query = source.get("query", f"SELECT * FROM {source.get('table', 'table')}")
    for chunk in pd.read_sql(query, engine, chunksize=chunk_size):
        yield chunk
    engine.dispose()


# ---------------------------------------------------------------------------
# Salesforce
# ---------------------------------------------------------------------------

@ConnectorRegistry.register("salesforce")
def _fetch_salesforce(source: Dict) -> pd.DataFrame:
    from simple_salesforce import Salesforce

    sf = Salesforce(
        username=source["username"],
        password=source["password"],
        security_token=source.get("security_token", ""),
        domain=source.get("domain", "login"),
    )
    result = sf.query_all(source["query"])
    records = result["records"]
    for record in records:
        record.pop("attributes", None)
    return pd.DataFrame(records)


@ConnectorRegistry.register("salesforce", streaming=True)
def _fetch_salesforce_streaming(source: Dict, chunk_size: int) -> Iterator[pd.DataFrame]:
    df = _fetch_salesforce(source)
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i : i + chunk_size]


# ---------------------------------------------------------------------------
# Synthetic Data
# ---------------------------------------------------------------------------

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer",
    "Michael", "Linda", "William", "Barbara", "David", "Elizabeth",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez",
]
COUNTRIES = [
    "USA", "UK", "Canada", "Australia", "Germany", "France",
    "Spain", "Italy", "Brazil", "Mexico", "Japan", "India",
]


def _generate_column(col_type: str, count: int) -> list:
    if col_type == "uuid":
        return [str(uuid.uuid4()) for _ in range(count)]
    if col_type == "email":
        return [
            f"{random.choice(FIRST_NAMES).lower()}.{random.choice(LAST_NAMES).lower()}@example.com"
            for _ in range(count)
        ]
    if col_type == "first_name":
        return [random.choice(FIRST_NAMES) for _ in range(count)]
    if col_type == "last_name":
        return [random.choice(LAST_NAMES) for _ in range(count)]
    if col_type == "date":
        start = datetime.now() - timedelta(days=365 * 5)
        return [(start + timedelta(days=random.randint(0, 365 * 5))).date() for _ in range(count)]
    if col_type == "currency":
        return [round(random.uniform(10, 10000), 2) for _ in range(count)]
    if col_type == "boolean":
        return [random.choice([True, False]) for _ in range(count)]
    if col_type == "country":
        return [random.choice(COUNTRIES) for _ in range(count)]
    if col_type.startswith("integer:"):
        parts = col_type.split(":")
        lo = int(parts[1]) if len(parts) > 1 else 0
        hi = int(parts[2]) if len(parts) > 2 else 100
        return [random.randint(lo, hi) for _ in range(count)]
    if col_type.startswith("string:"):
        length = int(col_type.split(":")[1]) if ":" in col_type else 10
        return ["".join(random.choices(string.ascii_letters, k=length)) for _ in range(count)]
    return [f"value_{i}" for i in range(count)]


@ConnectorRegistry.register("synthetic")
def _fetch_synthetic(source: Dict) -> pd.DataFrame:
    schema = source.get("schema", {})
    row_count = source.get("row_count", 1000)
    data = {col: _generate_column(ctype, row_count) for col, ctype in schema.items()}
    return pd.DataFrame(data)


@ConnectorRegistry.register("synthetic", streaming=True)
def _fetch_synthetic_streaming(source: Dict, chunk_size: int) -> Iterator[pd.DataFrame]:
    schema = source.get("schema", {})
    total = source.get("row_count", 1000)
    generated = 0
    while generated < total:
        n = min(chunk_size, total - generated)
        data = {col: _generate_column(ctype, n) for col, ctype in schema.items()}
        yield pd.DataFrame(data)
        generated += n


# ---------------------------------------------------------------------------
# Postgres write helper (used by streaming.py)
# ---------------------------------------------------------------------------

class PostgresWriter:
    @staticmethod
    def write(df: pd.DataFrame, config: Dict, if_exists: str = "replace") -> str:
        conn_str = (
            f"postgresql://{config['username']}:{config['password']}"
            f"@{config['host']}:{config.get('port', 5432)}/{config['database']}"
        )
        engine = create_engine(conn_str)
        df.to_sql(config["table"], engine, if_exists=if_exists, index=False)
        engine.dispose()
        return f"postgres://{config['host']}:{config.get('port', 5432)}/{config['database']}/{config['table']}"
