from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

#silver table



df = spark.read.format("delta").load("abfss://silver@storageawesome.dfs.core.windows.net/customers")
df.show()