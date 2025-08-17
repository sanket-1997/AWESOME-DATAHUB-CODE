import os

def get_library_path():
    full_path = os.getcwd()
    datahubPath = os.path.dirname(os.path.dirname(full_path))

    return datahubPath

print(get_library_path())

