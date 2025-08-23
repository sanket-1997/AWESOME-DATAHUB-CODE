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
    



