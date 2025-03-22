# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW mapeo_origen_campania_view
# MAGIC     AS SELECT * FROM silver_lakehouse.mapeo_origen_campania

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.mapeo_origen_campania
# MAGIC USING mapeo_origen_campania_view 
# MAGIC ON gold_lakehouse.mapeo_origen_campania.utm_source = mapeo_origen_campania_view.utm_source
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
