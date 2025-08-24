from pyspark.sql.functions import *
from pyspark.sql.types import *
from libs import azureauth as au
from libs import schema_handler as sh


env = au.get_env(spark)
bronze_table = dbutils.widgets.get("bronze_table")
print(bronze_table)
