# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW origen_campania_sales_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT 
# MAGIC     origen_campania AS nombre_origen_campania, 
# MAGIC     (SELECT IFNULL(MAX(utm_type), 'n/a') FROM gold_lakehouse.mapeo_origen_campania WHERE utm_source = origen_campania) AS tipo_campania, 
# MAGIC     (SELECT IFNULL(MAX(utm_channel), 'n/a') FROM gold_lakehouse.mapeo_origen_campania WHERE utm_source = origen_campania) AS canal_campania, 
# MAGIC     (SELECT IFNULL(MAX(utm_medium), 'n/a') FROM gold_lakehouse.mapeo_origen_campania WHERE utm_source = origen_campania) AS medio_campania,
# MAGIC     fec_procesamiento
# MAGIC FROM silver_lakehouse.sales
# MAGIC WHERE origen_campania IS NOT NULL
# MAGIC   AND origen_campania != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Crear una vista temporal con datos deduplicados y más recientes
# MAGIC CREATE OR REPLACE TEMPORARY VIEW deduplicated_origen_campania AS 
# MAGIC SELECT nombre_origen_campania, tipo_campania, canal_campania, medio_campania, fec_procesamiento
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         UPPER(TRIM(nombre_origen_campania)) AS nombre_origen_campania, 
# MAGIC         MAX(tipo_campania) AS tipo_campania, 
# MAGIC         MAX(canal_campania) AS canal_campania, 
# MAGIC         MAX(medio_campania) AS medio_campania,
# MAGIC         MAX(fec_procesamiento) AS fec_procesamiento,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY UPPER(TRIM(nombre_origen_campania))
# MAGIC             ORDER BY COALESCE(tipo_campania, ''), 
# MAGIC                      COALESCE(canal_campania, ''), 
# MAGIC                      COALESCE(medio_campania, '')
# MAGIC         ) AS rn
# MAGIC     FROM origen_campania_sales_view 
# MAGIC     WHERE nombre_origen_campania IS NOT NULL AND nombre_origen_campania <> ''
# MAGIC     GROUP BY nombre_origen_campania, tipo_campania, canal_campania, medio_campania, fec_procesamiento
# MAGIC ) WHERE rn = 1;  -- 🔹 Solo mantenemos una fila única por `nombre_origen_campania`
# MAGIC
# MAGIC
# MAGIC -- 2️⃣ 🔹 Insertar solo si el registro `-1` no existe
# MAGIC MERGE INTO gold_lakehouse.dim_origen_campania AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_origen_campania, 'n/a' AS tipo_campania, 'n/a' AS canal_campania, 'n/a' AS medio_campania, current_timestamp() AS fec_procesamiento
# MAGIC ) AS source
# MAGIC ON target.id_dim_origen_campania = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_origen_campania, tipo_campania, canal_campania, medio_campania, fec_procesamiento, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('n/a', 'n/a', 'n/a', 'n/a', current_timestamp(), current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 3️⃣ 🔹 **MERGE evitando duplicados en la 2da Carga**
# MAGIC MERGE INTO gold_lakehouse.dim_origen_campania AS target
# MAGIC USING (
# MAGIC     -- Solo tomamos registros nuevos o que han cambiado
# MAGIC     SELECT s.*
# MAGIC     FROM deduplicated_origen_campania s
# MAGIC     LEFT JOIN gold_lakehouse.dim_origen_campania t
# MAGIC         ON UPPER(TRIM(t.nombre_origen_campania)) = UPPER(TRIM(s.nombre_origen_campania))
# MAGIC     WHERE t.nombre_origen_campania IS NULL  -- 🔹 Solo nuevos registros
# MAGIC     OR (
# MAGIC         t.fec_procesamiento < s.fec_procesamiento  -- 🔹 Solo si es un dato más reciente
# MAGIC         OR COALESCE(t.tipo_campania, '') <> COALESCE(s.tipo_campania, '')
# MAGIC         OR COALESCE(t.canal_campania, '') <> COALESCE(s.canal_campania, '')
# MAGIC         OR COALESCE(t.medio_campania, '') <> COALESCE(s.medio_campania, '')
# MAGIC     )
# MAGIC ) AS source
# MAGIC ON UPPER(TRIM(target.nombre_origen_campania)) = UPPER(TRIM(source.nombre_origen_campania))
# MAGIC AND target.id_dim_origen_campania != -1  -- 🔹 Evita afectar el registro `-1`
# MAGIC
# MAGIC -- 🔹 **Actualizar solo si los valores han cambiado**
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.tipo_campania = source.tipo_campania,
# MAGIC         target.canal_campania = source.canal_campania,
# MAGIC         target.medio_campania = source.medio_campania,
# MAGIC         target.fec_procesamiento = source.fec_procesamiento,
# MAGIC         target.ETLupdatedDate = current_timestamp()
# MAGIC
# MAGIC -- 🔹 **Insertar solo registros realmente nuevos**
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_origen_campania, tipo_campania, canal_campania, medio_campania, fec_procesamiento, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_origen_campania, source.tipo_campania, source.canal_campania, source.medio_campania, source.fec_procesamiento, current_timestamp(), current_timestamp());
