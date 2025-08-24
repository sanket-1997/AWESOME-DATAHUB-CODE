import os
import yaml

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
    



