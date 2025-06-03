# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_estado_venta_view AS
# MAGIC SELECT DISTINCT
# MAGIC     CASE 
# MAGIC         WHEN etapa = 'Perdido' THEN 'PERDIDA'
# MAGIC         WHEN etapa = 'Matriculado' OR etapa = 'NEC' THEN 'GANADA'
# MAGIC         ELSE 'ABIERTA'
# MAGIC     END AS nombre_estado_venta
# MAGIC FROM silver_lakehouse.zohodeals
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC     CASE 
# MAGIC         WHEN etapa = 'Perdido' THEN 'PERDIDA'
# MAGIC         WHEN etapa = 'Matriculado' OR etapa = 'NEC' THEN 'GANADA'
# MAGIC         ELSE 'ABIERTA'
# MAGIC     END AS nombre_estado_venta
# MAGIC FROM silver_lakehouse.zohodeals_38b;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_estado_venta AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_estado_venta
# MAGIC ) AS source
# MAGIC ON target.id_dim_estado_venta = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_estado_venta, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_estado_venta, NULL, NULL);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_estado_venta AS target
# MAGIC USING dim_estado_venta_view AS source
# MAGIC ON UPPER(target.nombre_estado_venta) = UPPER(source.nombre_estado_venta)
# MAGIC AND target.id_dim_estado_venta != -1  -- 🔹 Evita afectar el registro `-1`
# MAGIC
# MAGIC -- 🔹 **Si ya existe, lo actualiza (si aplica)**
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.nombre_estado_venta = source.nombre_estado_venta
# MAGIC
# MAGIC -- 🔹 **Si no existe, lo inserta**
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_estado_venta, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_estado_venta, current_timestamp(), current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_estado_venta AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         nombre_estado_venta, 
# MAGIC         current_timestamp() AS ETLcreatedDate,
# MAGIC         current_timestamp() AS ETLupdatedDate
# MAGIC     FROM dim_estado_venta_view
# MAGIC ) AS source
# MAGIC ON target.nombre_estado_venta = source.nombre_estado_venta
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET target.ETLupdatedDate = source.ETLupdatedDate
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_estado_venta, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_estado_venta, source.ETLcreatedDate, source.ETLupdatedDate);
