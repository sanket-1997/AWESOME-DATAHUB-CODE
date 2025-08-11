# dhcorelibs/schema_utils.py
import os
import yaml

def resolve_contract_path(entity_filename: str, repo_root: str = None):
    """
    Resolve YAML path. Looks in:
      1) provided repo_root/dataquality/datacontracts/<entity_filename>
      2) current working directory ./dataquality/datacontracts/<entity_filename>
      3) DBFS path /Workspace/... or /dbfs/ if used. (simple fallback to /dbfs/dataquality/...)
    Returns the first existing path.
    """
    candidates = []
    if repo_root:
        candidates.append(os.path.join(repo_root, "dataquality", "datacontracts", entity_filename))
    candidates.append(os.path.join(os.getcwd(), "dataquality", "datacontracts", entity_filename))
    # dbfs fallback (when running on databricks local driver)
    candidates.append(os.path.join("/dbfs", "dataquality", "datacontracts", entity_filename))
    for p in candidates:
        if os.path.exists(p):
            return p
    raise FileNotFoundError(f"Contract file {entity_filename} not found. Tried: {candidates}")

def read_contract_yaml(entity_filename: str, repo_root: str = None) -> dict:
    """
    Example usage:
      contract = read_contract_yaml('dcm_...-dim_customer.yaml', repo_root='/Workspace/Repos/your/repo')
    """
    path = resolve_contract_path(entity_filename, repo_root=repo_root)
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def get_model_metadata(contract: dict, model_name: str) -> dict:
    model = contract.get('models', {}).get(model_name)
    if model is None:
        raise ValueError(f"Model {model_name} not found in contract")
    return model

def get_field_list(model_meta: dict):
    # support both list-of-dicts or dict-of-dicts (your original YAML is dict-of-dicts)
    fields = model_meta.get('fields')
    if isinstance(fields, dict):
        # convert to list of dicts preserving original keys -> 'name'
        out = []
        for k, v in fields.items():
            row = v.copy()
            row['name'] = k
            out.append(row)
        return out
    return fields or []
