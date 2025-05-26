# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_pais_view
# MAGIC     AS SELECT * FROM silver_lakehouse.dim_pais;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_pais
# MAGIC USING dim_pais_view 
# MAGIC ON gold_lakehouse.dim_pais.id = dim_pais_view.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
