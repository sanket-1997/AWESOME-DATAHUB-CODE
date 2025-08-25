from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from libs import utils
import yaml



#this will return field schema so we will get most of the value
def read_schema(env = None, layer = None, source = None, entity = None ) -> dict:
    """
    Reads schema fields and primary keys from the data contract YAML.
    Returns:
        (dict, list): field_schema, primary_keys
    """
    data = utils.open_yaml_file(utils.generate_dc_file_path(env, layer, source, entity))
    model_key = f"datahub_{env}_{layer}-{source}-{entity}"
    model_data = data["models"][model_key]

    field_schema = model_data["fields"]
    primary_keys = model_data.get("primaryKey", [])

    return field_schema, primary_keys


#this will return set of attributes and will be used for schema evolution
def extract_attributes(field_schema) -> list:
    return list(set(field_schema))


#this is to decide whether to evolve the schema or not
def schema_evolution_decider(source_attributes: list, target_attributes: list, contract_attributes: list) -> bool:

    source_set = set(source_attributes)
    target_set = set(target_attributes)
    contract_set = set(contract_attributes)

    # Find new attributes in source table that are not in target yet
    new_raw_attributes = source_set - target_set

    # If all new attributes are allowed by contract → True
    if new_raw_attributes.issubset(contract_set):
        return True
    else:
        return False


#this return data frame schema
def get_dataframe_schema(df, mode ="attributes_only"):
    if mode == "attributes_only":
        return[field.name for field in df.schema.fields]
    elif mode == "full":
        return [(field.name, field.dataType.simpleString()) for field in df.schema.fields]


#this to check it particular table exists or not
def uc_table_exists(spark, catalog: str, schema: str, table: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {catalog}.{schema}.{table}")
        return True
    except Exception as e:
        return False


def handle_schema_evolution( spark: SparkSession, source_df, target_path: str, contract_attributes: list, mode: str = "append"):
    """
    Evolves a Delta table schema if allowed by the schema contract.

    Args:
        spark (SparkSession): Active Spark session.
        source_df (DataFrame): Source DataFrame with latest schema.
        target_path (str): Path to the target Delta table.
        contract_attributes (list): List of allowed attributes as per contract.
        mode (str): Write mode ("append" or "overwrite"). Default = "append".

    Returns:
        bool: True if schema was evolved, False otherwise.
    """

    # Load target schema if table exists
    if DeltaTable.isDeltaTable(spark, target_path):
        target_df = spark.read.format("delta").load(target_path)
        target_attributes = target_df.columns
    else:
        target_attributes = []

    source_attributes = source_df.columns

    # Decide if evolution is allowed
    can_evolve = schema_evolution_decider(
        source_attributes, target_attributes, contract_attributes
    )

    if not can_evolve:
        print("Schema evolution not allowed by contract. Skipping write.")
        return False

    # Write with schema evolution enabled
    (
        source_df.write.format("delta")
        .mode(mode)
        .partitionBy("extract_timestamp")
        .option("mergeSchema", "true")   # <-- key part
        .save(target_path)
    )

    print("Schema evolved successfully.")
    return True





