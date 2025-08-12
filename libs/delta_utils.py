# libs/delta_utils.py

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as F

def delta_exists(spark, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False

def write_delta(df: DataFrame, path: str, mode: str="overwrite", partition_by: list=None):
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)

def enforce_schema_and_write(source_df: DataFrame, contract_fields: list, path: str, mode="overwrite"):
    """
    - Select fields in same order and cast to types defined in contract_fields.
    contract_fields: list of dicts with name and 'type' (string, long, date, boolean...)
    """
    df = source_df
    select_cols = []
    for f in contract_fields:
        fname = f['name']
        ftype = f.get('type', 'string').lower()
        if fname in df.columns:
            if ftype in ('string','varchar','char','text'):
                select_cols.append(col(fname).cast("string").alias(fname))
            elif ftype in ('long','int','bigint'):
                select_cols.append(col(fname).cast("long").alias(fname))
            elif ftype in ('double','float','decimal'):
                select_cols.append(col(fname).cast("double").alias(fname))
            elif ftype in ('boolean','bool'):
                select_cols.append(col(fname).cast("boolean").alias(fname))
            elif ftype in ('date','timestamp'):
                select_cols.append(col(fname).cast("timestamp").alias(fname))
            else:
                select_cols.append(col(fname).alias(fname))
        else:
            # Add null column if source missing
            select_cols.append(F.lit(None).cast("string").alias(fname))

    df_cast = df.select(*select_cols)
    write_delta(df_cast, path, mode=mode)
