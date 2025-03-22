# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

aircall_df = spark.read.json(f"{bronze_folder_path}/lakehouse/aircall/calls/{current_date}")
#aircall_df = spark.read.json(f"{bronze_folder_path}/lakehouse/aircall/calls/2024/11/29")

# COMMAND ----------

aircall_df = aircall_df.select("calls")

# COMMAND ----------

aircall_df = flatten(aircall_df)

# COMMAND ----------

aircall_df = aircall_df.select("calls_country_code_a2","calls_direction","calls_duration","calls_ended_at","calls_id","calls_missed_call_reason","calls_raw_digits","calls_started_at","calls_user_name")

# COMMAND ----------

aircall_df = aircall_df \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("calls_duration", col("calls_duration").cast(IntegerType())) \
    .withColumn("calls_ended_at", to_timestamp(from_unixtime(col("calls_ended_at"), 'yyyy-MM-dd HH:mm:ss'))) \
    .withColumn("calls_started_at", to_timestamp(from_unixtime(col("calls_started_at"), 'yyyy-MM-dd HH:mm:ss'))) \
    .withColumn("calls_id", col("calls_id").cast(StringType()))

# COMMAND ----------

for col in aircall_df.columns:
    aircall_df = aircall_df.withColumnRenamed(col, col.replace("calls_", ""))

# COMMAND ----------

for t in aircall_df.dtypes:
    if t[1] == 'string':
        aircall_df = aircall_df.withColumn(t[0], coalesce(t[0], lit('n/a')))

# COMMAND ----------

aircall_df = aircall_df.dropDuplicates()

# COMMAND ----------

aircall_df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE WITH SCHEMA EVOLUTION 
# MAGIC INTO silver_lakehouse.aircallcalls
# MAGIC USING source_view 
# MAGIC ON silver_lakehouse.aircallcalls.id = source_view.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC  
