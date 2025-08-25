import sys
import time
from datetime import datetime


from libs import utils
from libs import schema_handler

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import *


def filter_unique_rows(spark, df, str_schema, pk_columns):
    df.createOrReplaceTempView("data_with_seqnum")

    col_list = str_schema.replace(" STRING", "")
    str_unique_sql = f"""SELECT {col_list} 
                        FROM (
                            SELECT *, 
                            ROW_NUMBER() OVER (PARTITION BY {pk_columns} ORDER BY row_seqnum) AS row_num 
                            FROM data_with_seqnum ORDER BY {pk_columns}, row_reqnum desc
                        )WHERE row_num = 1  
                """ 
    print(str_unique_sql)
    dfRawUnique = spark.sql(str_unique_sql)
    return dfRawUnique


def fill_null_primary_keys(df, primary_keys):
    """
    Fill null values in primary key columns with defaults based on data type.
    
    - StringType → ""
    - Numeric (byte, short, int, long, float, double) → 0
    - DateType → "yyyy-MM-dd"
    """
    schema = df.schema
    for field in schema:
        if field.name in primary_keys:
            dtype = field.dataType

            if isinstance(dtype, StringType):
                print(f"Replacing null values in {field.name} with ''")
                df = df.withColumn(
                    field.name,
                    F.when(F.col(field.name).isNull(), "").otherwise(F.col(field.name))
                )

            elif isinstance(dtype, (ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType)):
                print(f"Replacing null values in {field.name} with 0")
                df = df.withColumn(
                    field.name,
                    F.when(F.col(field.name).isNull(), F.lit(0)).otherwise(F.col(field.name))
                )

            elif isinstance(dtype, DateType):
                print(f"Replacing null values in {field.name} with 'yyyy-MM-dd'")
                df = df.withColumn(
                    field.name,
                    F.when(F.col(field.name).isNull(), F.lit("yyyy-MM-dd")).otherwise(F.col(field.name))
                )
            else:
                pass

    return df


#cast columns coming from raw data to cleansed layer. 
def cast_columns(df, metadata: dict):
    for col_name, rules in metadata.items():
        dtype = rules["type"].lower()
        if dtype in ("int", "integer"):
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
        elif dtype == "string":
            df = df.withColumn(col_name, F.col(col_name).cast(StringType()))
        elif dtype == "double":
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
        elif dtype == "timestamp":
            df = df.withColumn(col_name, F.col(col_name).cast(TimestampType()))
        elif dtype == "date":
            df = df.withColumn(col_name, F.col(col_name).cast(DateType()))
        elif dtype =="long":
            df = df.withColumn(col_name, F.col(col_name).cast(LongType()))
        elif dtype =="float":
            df = df.withColumn(col_name, F.col(col_name).cast(FloatType()))
        elif dtype =="decimal":
            precision = rules.get("precision", 30)
            scale = rules.get("scale", 10)
            df = df.withColumn(col_name, F.col(col_name).cast(DecimalType(precision, scale)))
        elif dtype =="boolean":
            df = df.withColumn(col_name, F.col(col_name).cast(BooleanType()))
        elif dtype =="short":
            df =df.withColumn(col_name, F.col(col_name).cast(ShortType()))
        elif dtype =="byte":
            df = df.withColumn(col_name, F.col(col_name).cast(ByteType()))
        else:
            df = df.withColumn(col_name, F.col(col_name).cast(StringType()))
    
    return df
        

def clean_data(df, metadata : dict):
    for col_name, rules in metadata.items():
        pattern = rules.get("pattern", None)
        dtype = rules.get("type", "string")
        if pattern:
            default_value = utils.get_default_value(col_name, dtype)
            df = df.withColumn(
                col_name, F.when(F.col(col_name).rlike(pattern), F.col(col_name)).otherwise(F.lit(default_value))

            )
    
    return df




    
                
