#!/usr/bin/env python3
import os
import argparse
import pandas as pd
import yaml
import ast
from datacontract.data_contract import DataContract
from yaml.representer import SafeRepresenter
import re


DEFAULT_INPUT_FOLDER = "datamodels"
DEFAULT_OUTPUT_FOLDER = "datacontracts"


class InlineList(list):
    pass

def inline_list_representer(dumper, data):
    return dumper.represent_sequence('tag:yaml.org,2002:seq', data, flow_style=True)

yaml.add_representer(InlineList, inline_list_representer, Dumper=yaml.SafeDumper)

class QuotedString(str):
    pass

def quoted_str_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style="'")

yaml.add_representer(QuotedString, quoted_str_representer, Dumper=yaml.SafeDumper)

def parse_key_value(key_value_str):
    if not isinstance(key_value_str, str) or not key_value_str.strip():
        return {}
    if ":" not in key_value_str:
        return {}

    key, value = key_value_str.split(":", 1)
    key = key.strip()
    value = value.strip().lower()

    try:
        if value == 'true':
            value = True
        elif value == 'false':
            value = False
        elif value in ('none', 'null'):
            value = None
        elif re.fullmatch(r'-?\d+\.\d+', value):  # float pattern
                value = float(value)
        elif re.fullmatch(r'-?\d+', value):  # int pattern
            value = int(value)
    except ValueError:
        pass

    return {key: value}

def parse_dict_with_list(raw):
    """
    Parses 'valid_values: [1, 2]' or 'valid_values: ["a", "b"]'
    """
    if not isinstance(raw, str) or ':' not in raw:
        return {}
    
    key, value = raw.split(":", 1)
    key = key.strip()
    value = value.strip()
    try:
        parsed_value = ast.literal_eval(value) #try with python expression
        return {key: parsed_value}
    except Exception as e:
        return {key: str(value)}

def parse_lineage(raw):
    input_fields = []
    if pd.isna(raw):
        return input_fields

    try:
        entries = ast.literal_eval(raw)
    except Exception:
        txt = str(raw).strip()[1:-1]
        entries = [e.strip() for e in txt.split(',') if e.strip()]

    for entry in entries:
        parts = entry.split('.')
        if len(parts) < 3:
            continue
        field = parts[-1]
        name = parts[-2]
        namespace = ".".join(parts[:-2])
        input_fields.append({
            "namespace": namespace,
            "name": name,
            "field": field
        })
    return input_fields

def create_data_contract(data_model_path: str, output_folder: str):
    view_name = os.path.splitext(os.path.basename(data_model_path))[0]
    out_path = os.path.join(output_folder, f"dc_{view_name}.yaml")

    model_df = pd.read_excel(data_model_path, sheet_name="model")
    servers_df = pd.read_excel(data_model_path, sheet_name="servers")
    field_df = pd.read_excel(data_model_path, sheet_name="field")

    info_desc = model_df.at[0, "description"] if "description" in model_df.columns and pd.notna(model_df.at[0, "description"]) else ""

    contract = {
        "dataContractSpecification": "1.1.0",
        "id": "",
        "info": {
            "title": f"BDH Data Contract for {view_name}",
            "version": "2.0.0",
            "description": info_desc or "No description provided",
            "owner": "BDH",
            "status": "active",
            "contact": {
                "name": "John Doe (Data Product Owner)",
                "email": "abc@gmail.com"
            }
        },
        "servers": {},
        "models": {
            view_name: {
                "description": info_desc or "No description provided",
                "type": "view",
                "fields": {},
                "primaryKey": []
            }
        }
    }

    for _, srv in servers_df.iterrows():
        key = srv["server_name"]
        contract["servers"][key] = {
            "type": srv["type"],
            "host": srv["host"],
            "catalog": srv["catalog"]
        }

    for _, fld in field_df.iterrows():
        fld_name = fld["field_name"]
        raw_ex = fld.get("examples", "")
        examples = []

        if pd.notna(raw_ex):
            for e in str(raw_ex).split(","):
                e = e.strip()
                if not e:
                    continue
                try:
                    val = ast.literal_eval(e)
                except Exception:
                    val = e
                if isinstance(val, str):
                    val = QuotedString(val)
                examples.append(val)

        raw_lin = fld.get("lineage", None)
        input_fields = parse_lineage(raw_lin)

        fld_spec = {
            "description": fld.get("description", "") or "No description provided",
            "type": fld.get("type", "string"),
            "required": fld.get("required",False),
            "unique": fld.get("unique", False),
            "examples": InlineList(examples),
            "lineage": {
                "inputFields": input_fields
            },

        }

        try:
            if fld.get("primary"):
                contract["models"][view_name]["primaryKey"].append(fld_name)
        except Exception as e:
            print(e)

        contract["models"][view_name]["fields"][fld_name] = fld_spec

    os.makedirs(output_folder, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.dump(contract, f, default_flow_style=False, allow_unicode=True, sort_keys=False, Dumper=yaml.SafeDumper)

    print(f"Wrote {out_path}")

    # Run lint check
    try:
        result = DataContract(out_path).lint()
        for r in result.checks:
            print(f"{r.name:<40} {r.result}")
    except Exception as e:
        print(f"❌ Linting failed for {out_path}: {e}")

def main():
    #parser = argparse.ArgumentParser(description="Generate YAML data contracts from Excel models.")


    input_path = os.getcwd() +"/dataquality/" + DEFAULT_INPUT_FOLDER
    output_path = os.getcwd() +"/dataquality/" + DEFAULT_OUTPUT_FOLDER
    print(input_path)

    if not os.path.isdir(input_path):
        print(f"❌ Input folder not found: {input_path}")
        return

    excels = [f for f in os.listdir(input_path) if f.lower().endswith(".xlsx")]
    if not excels:
        print(f"⚠️ No .xlsx files found in {input_path}")
        return

    for fname in excels:
        path = os.path.join(input_path, fname)
        try:
            create_data_contract(path, output_path)
        except Exception as e:
            print(f"❌ Error processing {fname}: {e}")

if __name__ == "__main__":
    main()







