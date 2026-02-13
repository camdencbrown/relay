"""
Additional connectors for Relay
Complex sources and destinations
"""

import pandas as pd
import requests
from typing import Dict, Iterator
from sqlalchemy import create_engine
import uuid
from datetime import datetime, timedelta
import random
import string

class MySQLConnector:
    """MySQL source connector"""
    
    @staticmethod
    def fetch(config: Dict) -> pd.DataFrame:
        """Fetch data from MySQL database"""
        host = config["host"]
        port = config.get("port", 3306)
        database = config["database"]
        username = config["username"]
        password = config["password"]
        query = config.get("query", f"SELECT * FROM {config.get('table', 'table')}")
        
        # Create connection string
        connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        
        # Create engine and fetch
        engine = create_engine(connection_string)
        df = pd.read_sql(query, engine)
        engine.dispose()
        
        return df

class PostgresConnector:
    """Postgres destination connector"""
    
    @staticmethod
    def write(df: pd.DataFrame, config: Dict, options: Dict) -> str:
        """Write data to Postgres database"""
        host = config["host"]
        port = config.get("port", 5432)
        database = config["database"]
        username = config["username"]
        password = config["password"]
        table = config["table"]
        
        # How to handle existing data
        if_exists = config.get("if_exists", "replace")  # replace, append, fail
        
        # Create connection string
        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        # Create engine and write
        engine = create_engine(connection_string)
        df.to_sql(table, engine, if_exists=if_exists, index=False)
        engine.dispose()
        
        return f"postgres://{host}:{port}/{database}/{table}"

class APIConnector:
    """Generic REST API connector"""
    
    @staticmethod
    def fetch(config: Dict) -> pd.DataFrame:
        """Fetch from REST API"""
        url = config["url"]
        method = config.get("method", "GET")
        headers = config.get("headers", {})
        auth = config.get("auth", None)
        params = config.get("params", {})
        
        # Add auth if provided
        if auth:
            if auth["type"] == "bearer":
                headers["Authorization"] = f"Bearer {auth['token']}"
            elif auth["type"] == "basic":
                from requests.auth import HTTPBasicAuth
                auth = HTTPBasicAuth(auth["username"], auth["password"])
        
        # Make request
        response = requests.request(method, url, headers=headers, params=params, auth=auth)
        response.raise_for_status()
        
        # Parse response
        data = response.json()
        
        # Handle different response structures
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            # Try common patterns
            for key in ["data", "results", "items", "records"]:
                if key in data:
                    return pd.DataFrame(data[key])
            # Fall back to wrapping dict as single row
            return pd.DataFrame([data])
        else:
            raise ValueError(f"Unsupported response type: {type(data)}")


class SalesforceConnector:
    """Salesforce source connector using SOQL queries"""
    
    @staticmethod
    def fetch(config: Dict) -> pd.DataFrame:
        """Fetch data from Salesforce using SOQL"""
        from simple_salesforce import Salesforce
        
        # Authentication
        username = config["username"]
        password = config["password"]
        security_token = config.get("security_token", "")
        domain = config.get("domain", "login")  # or "test" for sandbox
        
        # Connect
        sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            domain=domain
        )
        
        # Execute SOQL query
        query = config["query"]  # e.g., "SELECT Id, Name, Amount FROM Opportunity"
        
        # Query can return large datasets, use bulk API for >2000 records
        result = sf.query_all(query)
        
        # Convert to DataFrame
        records = result['records']
        
        # Remove Salesforce metadata
        for record in records:
            record.pop('attributes', None)
        
        return pd.DataFrame(records)


class SyntheticDataGenerator:
    """Generate synthetic data for testing and demos"""
    
    # Sample data for realistic generation
    FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", 
                   "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica"]
    
    LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                  "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson"]
    
    COUNTRIES = ["USA", "UK", "Canada", "Australia", "Germany", "France", "Spain", "Italy",
                 "Brazil", "Mexico", "Japan", "South Korea", "India", "China"]
    
    @staticmethod
    def generate_streaming(config: Dict, chunk_size: int = 10000) -> Iterator[pd.DataFrame]:
        """
        Generate synthetic data in chunks for streaming
        Efficiently generates millions of rows without memory overflow
        """
        total_rows = config.get("row_count", 1000)
        schema = config.get("schema", {})
        
        rows_generated = 0
        
        while rows_generated < total_rows:
            # Generate chunk
            current_chunk_size = min(chunk_size, total_rows - rows_generated)
            chunk_data = {}
            
            for col_name, col_type in schema.items():
                chunk_data[col_name] = SyntheticDataGenerator._generate_column(
                    col_type, current_chunk_size
                )
            
            yield pd.DataFrame(chunk_data)
            rows_generated += current_chunk_size
    
    @staticmethod
    def _generate_column(col_type: str, count: int) -> list:
        """Generate a column of synthetic data"""
        
        if col_type == "uuid":
            return [str(uuid.uuid4()) for _ in range(count)]
        
        elif col_type == "email":
            return [f"{random.choice(SyntheticDataGenerator.FIRST_NAMES).lower()}.{random.choice(SyntheticDataGenerator.LAST_NAMES).lower()}@example.com" 
                    for _ in range(count)]
        
        elif col_type == "first_name":
            return [random.choice(SyntheticDataGenerator.FIRST_NAMES) for _ in range(count)]
        
        elif col_type == "last_name":
            return [random.choice(SyntheticDataGenerator.LAST_NAMES) for _ in range(count)]
        
        elif col_type == "date":
            start_date = datetime.now() - timedelta(days=365*5)
            return [(start_date + timedelta(days=random.randint(0, 365*5))).date() for _ in range(count)]
        
        elif col_type == "currency":
            return [round(random.uniform(10, 10000), 2) for _ in range(count)]
        
        elif col_type == "boolean":
            return [random.choice([True, False]) for _ in range(count)]
        
        elif col_type == "country":
            return [random.choice(SyntheticDataGenerator.COUNTRIES) for _ in range(count)]
        
        elif col_type.startswith("integer:"):
            # Format: "integer:min:max"
            parts = col_type.split(":")
            min_val = int(parts[1]) if len(parts) > 1 else 0
            max_val = int(parts[2]) if len(parts) > 2 else 100
            return [random.randint(min_val, max_val) for _ in range(count)]
        
        elif col_type.startswith("string:"):
            # Format: "string:length"
            length = int(col_type.split(":")[1]) if ":" in col_type else 10
            return [''.join(random.choices(string.ascii_letters, k=length)) for _ in range(count)]
        
        else:
            # Default: random strings
            return [f"value_{i}" for i in range(count)]
