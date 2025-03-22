# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW titulacion_budget_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT titulacion AS titulacion
# MAGIC
# MAGIC FROM 
# MAGIC     silver_lakehouse.budget

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_titulacion_budget
# MAGIC USING titulacion_budget_view 
# MAGIC ON gold_lakehouse.dim_titulacion_budget.titulacion = titulacion_budget_view.titulacion
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_titulacion_budget.titulacion)
# MAGIC VALUES (titulacion_budget_view.titulacion)
