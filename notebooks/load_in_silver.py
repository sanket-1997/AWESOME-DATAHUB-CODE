# Databricks notebook source
from libs import schema_handler
from libs import datafunctions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from libs import azureauth as au
from libs import schema_handler as sh

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# -------------------
# Run 1 (Initial load)
# -------------------
schema_target = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("effective_start_date", StringType(), True),
    StructField("effective_end_date", StringType(), True)
])

target_data = [
    (1, "Alice", "Finance", "2024-01-01", "9999-12-31"),
    (2, "Bob", "IT", "2024-01-01", "9999-12-31")
]
target = spark.createDataFrame(target_data, schema_target)

print("🏁 Target after Run 1")
target.show()

# -------------------
# Run 2 (Schema evolved: new column 'location')
# -------------------
schema_source = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("location", StringType(), True),  # NEW COLUMN
    StructField("effective_start_date", StringType(), True),
    StructField("effective_end_date", StringType(), True)
])

source_data = [
    (1, "Alice", "HR", "NY", "2024-02-01", "9999-12-31"),  # dept changed + new location
    (2, "Bob", "IT", "SF", "2024-02-01", "9999-12-31")     # dept same, new location
]
source = spark.createDataFrame(source_data, schema_source)

print("📥 Source in Run 2 (with evolved schema)")
source.show()

# -------------------
# Compare with !=
# -------------------
print("🔴 Using != (NULL handling fails when comparing location)")
changed_wrong = target.join(source, "emp_id") \
    .filter((target["dept"] != source["dept"]) | (F.lit(None) != source["location"]))  # location is NULL in target
changed_wrong.show()

# -------------------
# Compare with eqNullSafe
# -------------------
print("🟢 Using eqNullSafe (correct change detection incl. NULL vs non-NULL)")
changed_correct = target.join(source, "emp_id") \
    .filter(~target["dept"].eqNullSafe(source["dept"]) | ~F.lit(None).eqNullSafe(source["location"]))
changed_correct.show()


# COMMAND ----------

dfBronze = spark.read.table("datahub_dev_bronze.consumer.customers").filter("extract_timestamp > 'DH_CUSTOMERS_19000823_003108'")
display(dfBronze)

# COMMAND ----------

metadata= schema_handler.read_schema('dev','silver', 'consumer', 'dim_customers')
print(metadata)

# COMMAND ----------

dfBronze = datafunctions.clean_data(dfBronze, metadata)
dfBronze = datafunctions.cast_columns(dfBronze, metadata)

display(dfBronze)

# COMMAND ----------

for col, rules in a.items():
    print(col)
    print(rules)
    p=rules.get("pattern", None)
    if p:
        print(p)
    

# COMMAND ----------

df = spark.read.format("delta").load("abfss://bronze@storageawesum.dfs.core.windows.net/consumer/customers/")
#display(df)
print(schema_handler.get_dataframe_schema(df))  # Verify the correct function name or import



# COMMAND ----------


