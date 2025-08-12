import re
import json
import os
from jinja2 import Template
from bdhcoredbx import utils

def read_schema(entity: str, context={}) -> dict:
    libPath = utils.get_library_path() + f"\\schema\\avro\\{entity}.avro"

    with open(libPath) as file_:
        template = Template(file_.read())
        config_str = template.render(**context)
        config_dict = json.loads(config_str)
        fieldSchema = config_dict['fields']
        del config_dict['fields']
        avro_schema = config_dict
        return avro_schema, fieldSchema

def parse_avro_schema(avroSchema):
    strSchema = ""
    mergeCond = ""
    pkCols = ""

    for col in avroSchema:
        dataType = col["type"].upper()
        strSchema += ", " + col["name"] + " " + dataType if strSchema else col["name"] + " " + dataType
        dataType = "STRING" if dataType == "DATE" else dataType

        if "key" in col and col["key"]:
            colName = col["name"]
            condition = f"nvl(cleansed.{colName}, '$%&+#@!~') = nvl(raw.{colName}, '$%&+#@!~')"
            mergeCond += " and " + condition if mergeCond else condition
            pkCols += ", " + colName if pkCols else colName

    return strSchema.strip(), mergeCond.strip(), pkCols.strip()

def get_ref_sk_metadata(fieldSchema):
    skList = []
    for field in fieldSchema:
        if field.get("skField"):
            sk_fields = field["skField"].split(',')
            for i in range(len(sk_fields)):
                sk = {}
                skData = {}
                fieldNames = []
                refFields = []

                skField = sk_fields[i].strip()
                fieldName = field.get("name").strip()
                refField = field.get("refField").split(",")[i].strip()

                try:
                    refDimType = field.get("refDimensionType", "").split(',')[i].strip()
                except:
                    refDimType = 'type1'

                joinOnCondition = [fieldName, refField]
                fieldNames.append(fieldName)
                refFields.append(refField)

                sk_index = next((index for (index, item) in enumerate(skList) if item.get('field') == skField), None)

                if sk_index is None:
                    sk['field'] = skField
                    skData['fieldNames'] = fieldNames
                    skData['refFields'] = refFields
                    skData['refSkField'] = field.get("refSkField").split(',')[i].strip()
                    skData['refDimension'] = field.get("refDimension").split(',')[i].strip()
                    skData['refDimensionType'] = refDimType
                    skData['joinOnCondition'] = [joinOnCondition]
                    sk['metaData'] = skData
                    skList.append(sk)
                else:
                    skList[sk_index]['metaData']['fieldNames'].append(fieldName)
                    skList[sk_index]['metaData']['refFields'].append(refField)
                    skList[sk_index]['metaData']['joinOnCondition'].append(joinOnCondition)

    print('Reference SK List ---->', skList)
    return skList

def get_mergeCondition(fieldSchema, source_table="bronze", target_table="silver"):
    merge_conditions = []

    for ef in fieldSchema:
        try:
            if ef.get("key"):
                col = ef["name"]
                data_type = ef.get("athena_type", "").lower()

                if "char" in data_type or "string" in data_type:
                    condition = f"nvl('.'||{target_table}.{col}||'.', '$%&+#@!~') = nvl('.'||{source_table}.{col}||'.', '$%&+#@!~')"
                else:
                    condition = f"{target_table}.{col} = {source_table}.{col}"

                merge_conditions.append(condition)
        except Exception:
            pass

    merge_condition = " and ".join(merge_conditions)
    return merge_condition

def transform_join_condition(original_join_condition, lpad_length=15):
    transformed_conditions = []

    pattern = r"\((.*?)\)"
    conditions = re.findall(pattern, original_join_condition)

    for condition in conditions:
        transformed = []
        columns = condition.split("==")

        for column in columns:
            column = column.strip()
            transformed.append(
                f"f.lpad({column}, {lpad_length}, '0').cast('string')"
            )

        transformed_conditions.append(" == ".join(transformed))

    final_join_condition = ' & '.join(f"({cond})" for cond in transformed_conditions)
    return final_join_condition

def get_primary_keys(fieldSchema):
    primaryKeys = []
    for ef in fieldSchema:
        if ef.get("key"):
            primaryKeys.append(ef["name"])

    return primaryKeys
