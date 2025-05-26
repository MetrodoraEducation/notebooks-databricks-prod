# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_escenario_presupuesto_view AS
# MAGIC SELECT DISTINCT budget_ff.escenario as nombre_escenario
# MAGIC   FROM silver_lakehouse.budget_ff budget_ff;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_escenario_presupuesto AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_escenario
# MAGIC ) AS source
# MAGIC ON target.nombre_escenario = 'n/a'
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_escenario, etlcreateddate, etlupdateddate)
# MAGIC     VALUES ('n/a', current_timestamp(), current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_escenario_presupuesto AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT nombre_escenario
# MAGIC     FROM dim_escenario_presupuesto_view
# MAGIC     WHERE nombre_escenario <> 'n/a'
# MAGIC ) AS source
# MAGIC ON target.nombre_escenario = source.nombre_escenario
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_escenario, etlcreateddate, etlupdateddate)
# MAGIC     VALUES (
# MAGIC         source.nombre_escenario,
# MAGIC         current_timestamp(),
# MAGIC         current_timestamp()
# MAGIC     );
