# libs/contract_utils.py

import yaml
import os

def _to_fs_path(path: str) -> str:
    # support dbfs:/... and /dbfs/.. variants
    if path.startswith("dbfs:/"):
        return path.replace("dbfs:/", "/dbfs/")
    return path

def read_contract(yaml_path: str) -> dict:
    """Read a data contract YAML and return a python dict."""
    fs_path = _to_fs_path(yaml_path)
    with open(fs_path, 'r') as f:
        return yaml.safe_load(f)

def extract_model(contract: dict):
    """
    Return: (model_name, model_def, fields_dict, business_keys, primary_key, scd_type)
    """
    # pick the first model in models
    models = contract.get("models", {})
    if not models:
        raise ValueError("No models found in contract")
    model_name = next(iter(models))
    model = models[model_name]

    fields = model.get("fields", {})
    # convert to list of field dicts
    field_list = []
    for fname, fmeta in fields.items():
        entry = {"name": fname}
        entry.update(fmeta if isinstance(fmeta, dict) else {})
        field_list.append(entry)

    business_keys = model.get("businessKeys", [])
    primary_key = model.get("primaryKey", [])
    scd_type = contract.get("info", {}).get("description", "").strip().upper()  # SCD1 / SCD2 / FACT

    return model_name, model, field_list, business_keys, primary_key, scd_type
