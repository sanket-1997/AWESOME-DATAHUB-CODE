# Databricks notebook source
import os
from libs import utils as ut
from libs import azureauth as az
from pathlib import Path

# COMMAND ----------

env = az.get_env(spark)
base_path = ut.get_library_path()
sql_files_root_path = str(Path(base_path) / "catalogs" / f"datahub_{env}_gold" )
subfolders = os.listdir(sql_files_root_path)
print(subfolders)
print(sql_files_root_path)


# COMMAND ----------

sql_file_paths =[]

for subfolder in subfolders:
    sql_file_names = os.listdir(sql_files_root_path + "/" +subfolder)

    for sql_file_name in sql_file_names:
        sql_file_paths.append(sql_files_root_path + "/" + subfolder + "/" + sql_file_name)


print(sql_file_paths)


# COMMAND ----------

for subfolder in subfolders:
    spark.sql(f"""
              CREATE SCHEMA IF NOT EXISTS datahub_{env}_gold.{subfolder};     
              """  
    )

# COMMAND ----------

# MAGIC %md
# MAGIC can also implement this in future
# MAGIC
# MAGIC     spark.sql(f"""
# MAGIC               ALTER SCHEMA datahub_{env}_gold_{subfolder} OWNER TO 'owner-name';     
# MAGIC               """  
# MAGIC     )

# COMMAND ----------

errors = []

for file_path in sql_file_paths:
    with open(file_path, 'r') as file:
        sql_file_content = file.read()
        try:
            queries = sql_file_content.split(";")
            for query in queries:
                #adapt quaery to environment
                env_adapt_query = query.replace("datahub_dev_gold", f"datahub_{env}_gold")
                if len(env_adapt_query) > 10:
                    spark.sql(env_adapt_query)
                print(env_adapt_query)
        except Exception as e:
            print(f"Error on file {file_path}.")
            errors.append(f"Error executing {file_path}: {e}")
                


# COMMAND ----------

if len(errors) > 0:
    for error in errors:
        print(error.split("JVM stack trace")[0])
    raise Exception("Errors detected while deploying SQL views. Check notebook logs for details.")

