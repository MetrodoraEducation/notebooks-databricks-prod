# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

mapeo_sede_df = spark.read.parquet(f"{bronze_folder_path}/lakehouse/mapeo_sede/{current_date}")

# COMMAND ----------

mapeo_sede_df = mapeo_sede_df.fillna({'sede': ''}, subset=['sede'])

# COMMAND ----------

mapeo_sede_df.createOrReplaceTempView("mapeo_sede_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.mapeo_sede
# MAGIC USING mapeo_sede_view 
# MAGIC ON silver_lakehouse.mapeo_sede.sede = mapeo_sede_view.sede
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
