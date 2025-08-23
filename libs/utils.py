import os
import yaml

def get_library_path():
    full_path = os.path.realpath(__file__)
    datahubPath = os.path.dirname(os.path.dirname(full_path))

    return datahubPath


def generate_dc_file_path(env = None, layer = None, source = None, entity = None ):
    file_path = get_library_path() + f"\\schema\\datacontracts\\{layer}\{source}\datahub_{env}_{layer}-{source}-{entity}.yaml"

    return file_path


def open_yaml_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)
            return data
    except Exception as e:
        print(f"Exception: {e}")
        

    return Exception
    



