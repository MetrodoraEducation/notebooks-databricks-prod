# Databricks notebook source
# MAGIC %md
# MAGIC ### **ORIGEN_CLASSLIFE**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `id_Dim_Origen_SIS = -1` existe solo una vez con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.origenClasslife AS target
# MAGIC USING (
# MAGIC     SELECT -1 AS id_Dim_Origen_SIS, 'n/a' AS nombre_Origen_SIS, -1 AS api_Client, 'n/a' AS codigo_Origen_SIS
# MAGIC ) AS source
# MAGIC ON target.id_Dim_Origen_SIS = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (id_Dim_Origen_SIS, nombre_Origen_SIS, api_Client, codigo_Origen_SIS, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (-1, 'n/a', -1, 'n/a', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 2️⃣ 🔹 Insertar los valores predeterminados (`1, 2, 3`) solo si no existen
# MAGIC MERGE INTO gold_lakehouse.origenClasslife AS target
# MAGIC USING (
# MAGIC     SELECT 1 AS id_Dim_Origen_SIS, 'Classlife Formación Continua' AS nombre_Origen_SIS, 919 AS api_Client, 'CLFC' AS codigo_Origen_SIS UNION ALL
# MAGIC     SELECT 2, 'Classlife Formación Profesional', 0, 'CLFP' UNION ALL
# MAGIC     SELECT 3, 'Classlife Educación Superior', 0, 'CLHE'
# MAGIC ) AS source
# MAGIC ON target.id_Dim_Origen_SIS = source.id_Dim_Origen_SIS
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (id_Dim_Origen_SIS, nombre_Origen_SIS, api_Client, codigo_Origen_SIS, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.id_Dim_Origen_SIS, source.nombre_Origen_SIS, source.api_Client, source.codigo_Origen_SIS, current_timestamp(), current_timestamp());
# MAGIC
