# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW escenario_budget_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT escenario AS escenario
# MAGIC FROM 
# MAGIC     silver_lakehouse.budget

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_escenario_budget
# MAGIC USING escenario_budget_view 
# MAGIC ON gold_lakehouse.dim_escenario_budget.escenario = escenario_budget_view.escenario
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_escenario_budget.escenario)
# MAGIC VALUES (escenario_budget_view.escenario)
