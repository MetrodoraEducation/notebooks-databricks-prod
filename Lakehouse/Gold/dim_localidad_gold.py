# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW localidad_sales_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT localidad AS nombre_localidad
# MAGIC FROM 
# MAGIC     silver_lakehouse.sales;
# MAGIC
# MAGIC select * from localidad_sales_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `-1` existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_localidad AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_localidad
# MAGIC ) AS source
# MAGIC ON target.id_dim_localidad = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_localidad)
# MAGIC     VALUES ('n/a');
# MAGIC
# MAGIC -- 2️⃣ 🔹 Realizar el MERGE para actualizar o insertar nuevos registros de `localidad_sales_view`
# MAGIC MERGE INTO gold_lakehouse.dim_localidad AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT nombre_localidad FROM localidad_sales_view
# MAGIC ) AS source
# MAGIC ON target.nombre_localidad = source.nombre_localidad
# MAGIC
# MAGIC -- 🔹 Si el registro ya existe, no se hace nada
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (nombre_localidad, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_localidad, current_timestamp(), current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3️⃣ 🔹 Asegurar que solo haya un único ID `-1`
# MAGIC DELETE FROM gold_lakehouse.dim_localidad
# MAGIC WHERE nombre_localidad = 'n/a' AND id_dim_localidad <> -1;
