# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW motivo_cierre_sales_view AS 
# MAGIC     SELECT 
# MAGIC         DISTINCT motivo_cierre AS motivo_cierre
# MAGIC     FROM silver_lakehouse.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `-1` existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_motivo_cierre AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS motivo_cierre
# MAGIC ) AS source
# MAGIC ON target.id_dim_motivo_cierre = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (motivo_cierre)
# MAGIC     VALUES ('n/a');
# MAGIC
# MAGIC -- 2️⃣ 🔹 Realizar el MERGE para actualizar o insertar nuevos registros de `motivo_cierre_sales_view`
# MAGIC MERGE INTO gold_lakehouse.dim_motivo_cierre AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT motivo_cierre FROM motivo_cierre_sales_view
# MAGIC ) AS source
# MAGIC ON target.motivo_cierre = source.motivo_cierre
# MAGIC
# MAGIC -- 🔹 Si el registro ya existe, no se hace nada
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (motivo_cierre, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.motivo_cierre, current_timestamp(), current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3️⃣ 🔹 Asegurar que solo haya un único ID `-1`
# MAGIC DELETE FROM gold_lakehouse.dim_motivo_cierre
# MAGIC WHERE motivo_cierre = 'n/a' AND id_dim_motivo_cierre <> -1;
