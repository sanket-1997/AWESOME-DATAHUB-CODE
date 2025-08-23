# Databricks notebook source
from libs import schema_handler

# COMMAND ----------

print(schema_handler.read_schema('dev','silver', 'consumer', 'dim_customer'))
