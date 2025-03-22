# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_nacionalidad_view
# MAGIC     AS SELECT * FROM gold_lakehouse.dim_pais;
# MAGIC
# MAGIC SELECT * FROM dim_nacionalidad_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_nacionalidad
# MAGIC USING dim_nacionalidad_view 
# MAGIC ON gold_lakehouse.dim_nacionalidad.id = dim_nacionalidad_view.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
