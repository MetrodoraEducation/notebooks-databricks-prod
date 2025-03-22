# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

mapeo_origen_campania_df = spark.read.parquet(f"{bronze_folder_path}/lakehouse/mapeo_origen_campania/{current_date}")

# COMMAND ----------

mapeo_origen_campania_df.createOrReplaceTempView("mapeo_origen_campania_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.mapeo_origen_campania
# MAGIC USING mapeo_origen_campania_view 
# MAGIC ON silver_lakehouse.mapeo_origen_campania.utm_source = mapeo_origen_campania_view.utm_source
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from silver_lakehouse.mapeo_origen_campania

# COMMAND ----------

#display(mapeo_origen_campania_df)

# COMMAND ----------

#mapeo_origen_campania_df.printSchema()
