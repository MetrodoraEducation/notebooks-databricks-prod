# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

mapeo_modalidad_df = spark.read.parquet(f"{bronze_folder_path}/lakehouse/mapeo_modalidad/{current_date}")

# COMMAND ----------

mapeo_modalidad_df = mapeo_modalidad_df.fillna({'modalidad': ''}, subset=['modalidad'])

# COMMAND ----------

mapeo_modalidad_df.createOrReplaceTempView("mapeo_modalidad_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.mapeo_modalidad
# MAGIC USING mapeo_modalidad_view 
# MAGIC ON silver_lakehouse.mapeo_modalidad.modalidad = mapeo_modalidad_view.modalidad
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
