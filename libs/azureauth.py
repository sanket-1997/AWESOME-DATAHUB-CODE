from pyspark.sql.functions import *


def get_env(spark):
    platform_env = spark.conf.get("spark.databricks.workspaceUrl").split(".")[0].split("-")[-1]
    env = 'prod' if '1908403585453078' not in platform_env else 'dev'
    return env