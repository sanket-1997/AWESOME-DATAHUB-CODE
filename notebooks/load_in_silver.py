# Databricks notebook source
from libs import schema_handler
from libs import datafunctions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from libs import azureauth as au
from libs import schema_handler as sh

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


