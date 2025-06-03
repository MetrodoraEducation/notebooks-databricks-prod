# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_MODALIDAD**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW modalidad_sales_view AS 
# MAGIC         SELECT DISTINCT 
# MAGIC                 modalidad AS nombre_modalidad
# MAGIC         FROM gold_lakehouse.dim_producto
# MAGIC         WHERE modalidad IS NOT NULL
# MAGIC         AND modalidad <> ''
# MAGIC         AND modalidad <> 'n/a';
# MAGIC
# MAGIC --select * from modalidad_sales_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ Asegurar que el ID -1 solo exista una vez con valores 'n/a'
# MAGIC MERGE INTO gold_lakehouse.dim_modalidad AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_modalidad, CURRENT_TIMESTAMP AS ETLcreatedDate, CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC ) AS source
# MAGIC ON target.id_dim_modalidad = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_modalidad, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('n/a', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
# MAGIC
# MAGIC -- 2️⃣ Actualizar registros existentes si cambia el código, sin afectar ETLcreatedDate
# MAGIC MERGE INTO gold_lakehouse.dim_modalidad AS target
# MAGIC USING modalidad_sales_view AS source
# MAGIC ON UPPER(target.nombre_modalidad) = UPPER(source.nombre_modalidad)
# MAGIC AND target.id_dim_modalidad != -1  -- 🔹 Evita actualizar el ID -1
# MAGIC
# MAGIC WHEN MATCHED AND target.nombre_modalidad <> source.nombre_modalidad THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.nombre_modalidad = source.nombre_modalidad,
# MAGIC         target.ETLupdatedDate = CURRENT_TIMESTAMP;
# MAGIC
# MAGIC -- 3️⃣ Insertar nuevos registros sin tocar el id_dim_modalidad
# MAGIC MERGE INTO gold_lakehouse.dim_modalidad AS target
# MAGIC USING modalidad_sales_view AS source
# MAGIC ON UPPER(target.nombre_modalidad) = UPPER(source.nombre_modalidad)
# MAGIC AND target.id_dim_modalidad != -1  -- 🔹 Evita modificar el -1
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_modalidad, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_modalidad, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
# MAGIC
# MAGIC -- 4️⃣ Eliminar duplicados de 'n/a' si aparecen por error
# MAGIC DELETE FROM gold_lakehouse.dim_modalidad
# MAGIC WHERE nombre_modalidad = 'n/a' AND id_dim_modalidad <> -1;
# MAGIC
