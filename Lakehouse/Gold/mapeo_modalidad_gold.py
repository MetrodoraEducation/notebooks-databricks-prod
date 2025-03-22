# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW mapeo_modalidad_view
# MAGIC     AS SELECT * FROM silver_lakehouse.mapeo_modalidad

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.mapeo_modalidad
# MAGIC USING mapeo_modalidad_view 
# MAGIC ON gold_lakehouse.mapeo_modalidad.modalidad = mapeo_modalidad_view.modalidad
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC
