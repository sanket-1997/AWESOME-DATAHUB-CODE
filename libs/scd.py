# libs/scd.py

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, concat_ws, md5, row_number, lit, current_date, date_sub
from pyspark.sql.types import StringType

def compute_hash(df: DataFrame, cols: list, hash_col="_hash"):
    """Return df with a md5 hash column for the provided cols."""
    return df.withColumn(hash_col, md5(concat_ws("||", *[F.coalesce(col(c).cast("string"), lit("")) for c in cols])))

def get_max_sk(spark, target_path: str, sk_col: str):
    if not DeltaTable.isDeltaTable(spark, target_path):
        return 0
    df = spark.read.format("delta").load(target_path)
    maxv = df.select(F.max(col(sk_col))).collect()[0][0]
    return 0 if maxv is None else int(maxv)

def scd1_upsert(spark, target_path: str, source_df: DataFrame, business_keys: list, all_cols: list, sk_col: str=None, partition_by: list=None):
    """
    SCD1: update attributes for existing business key, insert new rows (assign SK if needed).
    - target_path: delta path
    - source_df: cleansed DataFrame with contract columns
    - all_cols: all columns to be stored in target table (including SK if present)
    - business_keys: list of business key column names
    - sk_col: name of surrogate key column (if present in contract)
    """
    # ensure source has same columns order
    src = source_df.select(*all_cols) if set(all_cols).issubset(set(source_df.columns)) else source_df

    if not DeltaTable.isDeltaTable(spark, target_path):
        # create new table
        if sk_col:
            # assign SK starting from 1
            w = Window.orderBy(*business_keys)
            src_with_sk = src.withColumn(sk_col, row_number().over(w).cast("long"))
            src_with_sk.write.format("delta").mode("overwrite").save(target_path)
        else:
            src.write.format("delta").mode("overwrite").save(target_path)
        return

    # target exists
    delta_tbl = DeltaTable.forPath(spark, target_path)
    # Update existing rows (match by business keys)
    join_cond = " AND ".join([f"t.{k} = s.{k}" for k in business_keys])
    # create update set dict for non-key columns
    update_cols = {c: f"s.{c}" for c in all_cols if c not in business_keys and c != sk_col}
    # Perform MERGE update for matched rows (no insert)
    delta_tbl.alias("t").merge(
        source_df.alias("s"),
        join_cond
    ).whenMatchedUpdate(set=update_cols).execute()

    # Insert new rows (left_anti join)
    target_df = spark.read.format("delta").load(target_path)
    new_rows = source_df.alias("s").join(target_df.alias("t"), on=business_keys, how='left_anti').select("s.*")
    if new_rows.rdd.isEmpty():
        return

    if sk_col:
        max_sk = get_max_sk(spark, target_path, sk_col)
        w = Window.orderBy(*business_keys)
        new_with_sk = new_rows.withColumn("_rn", row_number().over(w)).withColumn(sk_col, col("_rn") + lit(max_sk)).drop("_rn")
        # append
        new_with_sk.select(*all_cols).write.format("delta").mode("append").save(target_path)
    else:
        new_rows.select(*all_cols).write.format("delta").mode("append").save(target_path)


def scd2_upsert(spark, target_path: str, source_df: DataFrame, business_keys: list, attribute_cols: list, sk_col: str,
                effective_start_col: str="effective_start_date", effective_end_col: str="effective_end_date",
                is_active_col: str="is_active", max_end_date: str="9999-12-31"):
    """
    SCD2 logic (two-step):
     1) Deactivate current active records that have changed (set is_active=false, effective_end_date = date_sub(src_start,1))
     2) Insert new versions for changed or new business keys, assigning new surrogate keys.
    """
    # ensure contract columns present in source_df (effective start if present)
    src = source_df
    # pick a start date column on source, else use current_date()
    if effective_start_col not in src.columns:
        src = src.withColumn(effective_start_col, current_date())

    # compute hashes for attribute comparison
    src_hash = compute_hash(src, attribute_cols, "_src_hash")

    # load target if exists, else create new full table with all src rows as first versions
    if not DeltaTable.isDeltaTable(spark, target_path):
        # first load: assign SK starting from 1, set effective_end_date to max_end_date and is_active true
        max_sk = 0
        w = Window.orderBy(*business_keys)
        first_batch = src_hash.withColumn(sk_col, row_number().over(w).cast("long")) \
                              .withColumn(effective_end_col, F.lit(max_end_date)) \
                              .withColumn(is_active_col, F.lit(True)) \
                              .withColumn("_hash", col("_src_hash"))
        # write
        first_batch.write.format("delta").mode("overwrite").save(target_path)
        return

    # target exists
    delta_tbl = DeltaTable.forPath(spark, target_path)
    # load only active records for comparison
    target_active = spark.read.format("delta").load(target_path).filter(col(is_active_col) == True)
    # compute hash on target active
    target_active_hash = compute_hash(target_active, attribute_cols, "_tgt_hash")

    # Identify changed rows: join source to active target by business key where hashes differ
    join_expr = [src_hash[k] == target_active_hash[k] for k in business_keys]
    # Build join conditions for DataFrame join
    on_cond = " AND ".join([f"s.{k} = t.{k}" for k in business_keys])
    joined = src_hash.alias("s").join(target_active_hash.alias("t"), on=[src_hash[k] == target_active_hash[k] for k in business_keys], how='inner')

    changed_src = joined.filter(col("_src_hash") != col("_tgt_hash")).select("s.*")

    # Step 1: deactivate matching active records for those changed business keys using MERGE
    if not changed_src.rdd.isEmpty():
        # Merge only those changed keys to set is_active = false and effective_end_date = date_sub(s.effective_start_date, 1)
        delta_tbl.alias("t").merge(
            changed_src.alias("s"),
            on_cond
        ).whenMatchedUpdate(
            condition = "t.{is_active} = true".format(is_active=is_active_col),
            set = {
                is_active_col: "false",
                effective_end_col: f"date_sub(s.{effective_start_col}, 1)"
            }
        ).execute()

    # Step 2: Insert new rows for new or changed business keys (do this after step 1 so active rows reflect deactivation)
    target_active_after = spark.read.format("delta").load(target_path).filter(col(is_active_col) == True)
    # rows that need to be inserted are those in source that do not match any active target (left_anti)
    to_insert = src_hash.alias("s").join(target_active_after.alias("t"), on=[src_hash[k] == target_active_after[k] for k in business_keys], how='left_anti').select("s.*")

    if to_insert.rdd.isEmpty():
        return

    # assign new SKs
    max_sk = get_max_sk(spark, target_path, sk_col)
    w = Window.orderBy(*business_keys)
    to_insert_with_sk = to_insert.withColumn("_rn", row_number().over(w)).withColumn(sk_col, col("_rn") + lit(max_sk)).drop("_rn")
    to_insert_final = to_insert_with_sk.withColumn(effective_end_col, F.lit(max_end_date)) \
                                       .withColumn(is_active_col, F.lit(True)) \
                                       .withColumn("_hash", col("_src_hash"))
    # append
    to_insert_final.write.format("delta").mode("append").save(target_path)
