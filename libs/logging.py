from pyspark.sql import Row
from libs import azureauth

def write_logs(spark, job_run_id, timestamp, pipeline_name, stage, entity, file_name, status, message):
  log_data = [Row(job_run_id=job_run_id, timestamp=timestamp, pipeline_name=pipeline_name, stage=stage, entity=entity, file_name=file_name, status=status, message=message)]
  log_df = spark.createDataFrame(log_data)
  #log_df.write.format("delta").mode("append").saveAsTable

  return None



print(azureauth.get_env(spark))