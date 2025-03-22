# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_ENTIDAD_LEGAL**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_entidad_legal_view AS
# MAGIC SELECT DISTINCT
# MAGIC     TRIM(institucion) AS nombre_Institucion
# MAGIC FROM silver_lakehouse.sales
# MAGIC WHERE institucion IS NOT NULL AND institucion <> '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `idDimEntidadLegal = -1` existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_entidad_legal AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_Institucion, 'n/a' AS codigo_Entidad_Legal
# MAGIC ) AS source
# MAGIC ON target.nombre_Institucion = 'n/a'
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Institucion, codigo_Entidad_Legal, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('n/a', 'n/a', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 2️⃣ 🔹 MERGE para insertar o actualizar entidades legales, excluyendo `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_entidad_legal AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT 
# MAGIC         TRIM(institucion) AS nombre_Institucion,
# MAGIC         CASE 
# MAGIC             WHEN TRIM(institucion) = 'CESIF' THEN 'CF'
# MAGIC             WHEN TRIM(institucion) = 'ISEP' THEN 'IP'
# MAGIC             -- Agregar más reglas aquí si se identifican más códigos
# MAGIC             ELSE NULL 
# MAGIC         END AS codigo_Entidad_Legal,
# MAGIC         current_timestamp() AS ETLcreatedDate,
# MAGIC         current_timestamp() AS ETLupdatedDate
# MAGIC     FROM silver_lakehouse.sales
# MAGIC     WHERE institucion IS NOT NULL 
# MAGIC       AND institucion <> '' 
# MAGIC       AND institucion <> 'n/a' -- Evitar modificar el registro especial
# MAGIC ) AS source
# MAGIC ON target.nombre_Institucion = source.nombre_Institucion
# MAGIC
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.codigo_Entidad_Legal = source.codigo_Entidad_Legal,
# MAGIC         target.ETLupdatedDate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Institucion, codigo_Entidad_Legal, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_Institucion, source.codigo_Entidad_Legal, source.ETLcreatedDate, source.ETLupdatedDate);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold_lakehouse.dim_entidad_legal (nombre_Institucion, codigo_Entidad_Legal, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 
# MAGIC     t.nombre_Institucion, 
# MAGIC     t.codigo_Entidad_Legal, 
# MAGIC     current_timestamp(), 
# MAGIC     current_timestamp()
# MAGIC FROM (
# MAGIC     VALUES 
# MAGIC         ('CESIF', 'CF'),
# MAGIC         ('UNIVERSANIDAD', 'US'),
# MAGIC         ('OCEANO', 'OC'),
# MAGIC         ('ISEP', 'IP'),
# MAGIC         ('METRODORA LEARNING', 'ML'),
# MAGIC         ('TROPOS', 'TP'),
# MAGIC         ('PLAN EIR', 'PE'),
# MAGIC         ('IEPP', 'IE'),
# MAGIC         ('CIEP', 'CI'),
# MAGIC         ('METRODORA FP', 'MF'),
# MAGIC         ('SAIUS', 'SA'),
# MAGIC         ('ENTI', 'EN'),
# MAGIC         ('METRODORAFP ALBACETE', 'AB'),
# MAGIC         ('METRODORAFP AYALA', 'AY'),
# MAGIC         ('METRODORAFP CÁMARA', 'CA'),
# MAGIC         ('METRODORAFP EUSES', 'EU'),
# MAGIC         ('OCEANO EXPO-ZARAGOZA', 'EX'),
# MAGIC         ('OCEANO PORCHES-ZARAGOZA', 'PO'),
# MAGIC         ('METRODORAFP GIJÓN', 'GI'),
# MAGIC         ('METRODORAFP LOGROÑO', 'LO'),
# MAGIC         ('METRODORAFP SANTANDER', 'ST'),
# MAGIC         ('METRODORAFP VALLADOLID', 'VL'),
# MAGIC         ('METRODORAFP MADRID-RIO', 'RI')
# MAGIC ) AS t(nombre_Institucion, codigo_Entidad_Legal)
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM gold_lakehouse.dim_entidad_legal target 
# MAGIC     WHERE target.codigo_Entidad_Legal = t.codigo_Entidad_Legal
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW entidad_legal_view AS
# MAGIC SELECT DISTINCT 
# MAGIC     TRIM(entidad_Legal) AS nombre_Institucion
# MAGIC FROM gold_lakehouse.dim_producto
# MAGIC WHERE entidad_Legal IS NOT NULL AND entidad_Legal <> ''
# MAGIC AND entidad_Legal NOT IN (SELECT nombre_Institucion FROM gold_lakehouse.dim_entidad_legal);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_entidad_legal AS target
# MAGIC USING (
# MAGIC     SELECT nombre_Institucion, UPPER(SUBSTRING(nombre_Institucion, 1, 2)) AS codigo_Entidad_Legal
# MAGIC     FROM entidad_legal_view
# MAGIC ) AS source
# MAGIC ON target.nombre_Institucion = source.nombre_Institucion
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Institucion, codigo_Entidad_Legal, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_Institucion, source.codigo_Entidad_Legal, current_timestamp(), current_timestamp());
