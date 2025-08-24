# Databricks notebook source
from libs import schema_handler
from libs import datafunctions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from libs import azureauth as au
from libs import schema_handler as sh

# COMMAND ----------

dfRaw = spark.read.table("datahub_dev_bronze.consumer.orders")
display(dfRaw)
dfRaw.withColumn("quantity", lit(1)).write.mode("append").saveAsTable("datahub_dev_silver.consumer.dim_customers")

# COMMAND ----------

a= schema_handler.read_schema('dev','silver', 'consumer', 'dim_customers')
print(a)

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


