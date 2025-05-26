# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW institucion_sales_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT 
# MAGIC     institucion AS nombre_institucion
# MAGIC   FROM silver_lakehouse.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `-1` existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_institucion AS target
# MAGIC USING (SELECT 'n/a' AS nombre_institucion) AS source
# MAGIC ON target.id_dim_institucion = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_institucion) VALUES ('n/a');
# MAGIC
# MAGIC -- 2️⃣ 🔹 Insertar nuevas instituciones de `institucion_sales_view`
# MAGIC MERGE INTO gold_lakehouse.dim_institucion AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT TRIM(UPPER(nombre_institucion)) AS nombre_institucion
# MAGIC     FROM institucion_sales_view
# MAGIC ) AS source
# MAGIC ON target.nombre_institucion = source.nombre_institucion
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (nombre_institucion, ETLcreatedDate, ETLupdatedDate) 
# MAGIC     VALUES (source.nombre_institucion, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
# MAGIC
# MAGIC -- 3️⃣ 🔹 Asegurar que solo haya un único ID `-1`
# MAGIC DELETE FROM gold_lakehouse.dim_institucion
# MAGIC WHERE nombre_institucion = 'n/a' AND id_dim_institucion <> -1;

# COMMAND ----------

# DBTITLE 1,dim_producto
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW entidad_legal_view AS
# MAGIC SELECT DISTINCT
# MAGIC                CASE 
# MAGIC                     WHEN entidad_legal IS NOT NULL AND entidad_legal != '' THEN TRIM(UPPER(entidad_legal))
# MAGIC                     ELSE 'n/a'
# MAGIC                     END nombre_institucion
# MAGIC FROM gold_lakehouse.dim_producto;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_institucion AS target
# MAGIC USING entidad_legal_view AS source
# MAGIC ON TRIM(UPPER(target.nombre_institucion)) = TRIM(UPPER(source.nombre_institucion))
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (nombre_institucion, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (source.nombre_institucion, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
