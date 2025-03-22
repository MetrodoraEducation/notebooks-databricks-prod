# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW mapeo_sede_view
# MAGIC     AS SELECT * FROM silver_lakehouse.mapeo_sede

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.mapeo_sede
# MAGIC USING mapeo_sede_view 
# MAGIC ON gold_lakehouse.mapeo_sede.sede = mapeo_sede_view.sede
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
