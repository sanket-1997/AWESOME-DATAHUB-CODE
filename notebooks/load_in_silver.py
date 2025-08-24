# Databricks notebook source
from libs import schema_handler
from libs import datafunctions

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
display(df)
