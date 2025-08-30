import os
import yaml
from datetime import datetime, date

import os
from pathlib import Path

def get_library_path():
    # safer for Databricks: fall back if __file__ is missing
    try:
        full_path = os.path.realpath(__file__)
        datahubPath = os.path.dirname(os.path.dirname(full_path))
    except NameError:
        # fallback for Databricks notebooks
        datahubPath = os.getcwd()
    return datahubPath

def generate_dc_file_path(env=None, layer=None, source=None, entity=None):
    base_path = Path(get_library_path())
    file_path = base_path / "schema" / "datacontracts" / layer / source / f"datahub_{env}_{layer}-{source}-{entity}.yaml"
    return str(file_path)

def str_to_date(date_str: str)-> date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def open_yaml_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)
            return data
    except Exception as e:
        print(f"Exception: {e}")
        

    return Exception


def get_default_value(col_name: str, dtype: str):
    dtype = dtype.lower()
    
    # handle specific business columns first
    if "email" in col_name.lower():
        return "unknown@example.com"
    elif "phone" in col_name.lower():
        return "0000000000"

    
    # fallback by type
    if dtype in ("int", "integer", "long", "decimal", "float", "double", "short", "byte"):
        return 0
    elif dtype == "date":
        return "1800-01-01"
    elif dtype == "timestamp":
        return "1800-01-01 00:00:00"
    else:
        return ""  # generic default for strings
    

def format_timestamp(ts: str) -> str:
    """
    Converts timestamp string like '2025-08-24T11:23:39.0000000'
    to '20250824_112339'.
    """
    # Parse with microseconds (ignore trailing zeros beyond 6 digits)
    dt = datetime.strptime(ts[:26], "%Y-%m-%dT%H:%M:%S.%f")
    return dt.strftime("%Y%m%d_%H%M%S")
    



