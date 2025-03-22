# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_PROGRAMA**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_programa_view AS
# MAGIC SELECT DISTINCT
# MAGIC     UPPER(codigo_programa) AS cod_Programa,
# MAGIC     TRIM(UPPER(area_title)) AS nombre_Programa,
# MAGIC     TRIM(UPPER(degree_title)) AS tipo_Programa,
# MAGIC     TRIM(UPPER(entidad_legal)) AS entidad_Legal,
# MAGIC     TRIM(UPPER(especialidad)) AS especialidad,
# MAGIC     TRIM(UPPER(vertical)) AS vertical,
# MAGIC     TRIM(UPPER(nombre_del_programa_oficial_completo)) AS nombre_Programa_Completo,
# MAGIC     MAX(TRY_CAST(fecha_creacion AS TIMESTAMP)) AS ETLcreatedDate,
# MAGIC     MAX(TRY_CAST(ultima_actualizacion AS TIMESTAMP)) AS ETLupdatedDate
# MAGIC FROM silver_lakehouse.classlifetitulaciones
# MAGIC WHERE codigo_programa IS NOT NULL
# MAGIC   AND codigo_programa != ''
# MAGIC GROUP BY 
# MAGIC     cod_Programa, nombre_Programa, tipo_Programa, entidad_Legal, especialidad, vertical, nombre_Programa_Completo;
# MAGIC
# MAGIC select * from dim_programa_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cod_Programa, COUNT(*) AS total_duplicados
# MAGIC FROM dim_programa_view
# MAGIC GROUP BY cod_Programa
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ Asegurar que el registro `-1` siempre existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_programa AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS cod_Programa, 'n/a' AS nombre_Programa, 'n/a' AS tipo_Programa, 'n/a' AS entidad_Legal, 
# MAGIC            'n/a' AS especialidad, 'n/a' AS vertical, 'n/a' AS nombre_Programa_Completo
# MAGIC ) AS source
# MAGIC ON target.id_Dim_Programa = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (cod_Programa, nombre_Programa, tipo_Programa, entidad_Legal, especialidad, vertical, nombre_Programa_Completo, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 2️⃣ MERGE principal para `dim_programa`, asegurando que el `-1` no se modifique
# MAGIC MERGE INTO gold_lakehouse.dim_programa AS target
# MAGIC USING (
# MAGIC     WITH source_deduplicated AS (
# MAGIC         SELECT *, ROW_NUMBER() OVER (
# MAGIC             PARTITION BY cod_Programa, nombre_Programa_Completo 
# MAGIC             ORDER BY ETLupdatedDate DESC  -- 🔹 Nos quedamos con el registro más reciente
# MAGIC         ) AS rn
# MAGIC         FROM dim_programa_view 
# MAGIC         WHERE cod_Programa <> 'n/a'
# MAGIC     )
# MAGIC     SELECT cod_Programa, nombre_Programa, tipo_Programa, entidad_Legal, especialidad, vertical, nombre_Programa_Completo, ETLcreatedDate, ETLupdatedDate
# MAGIC     FROM source_deduplicated
# MAGIC     WHERE rn = 1  -- 🔹 Solo la fila más reciente por cada `cod_Programa` y `nombre_Programa_Completo`
# MAGIC ) AS source
# MAGIC ON UPPER(TRIM(target.cod_Programa)) = UPPER(TRIM(source.cod_Programa)) 
# MAGIC    AND UPPER(TRIM(target.nombre_Programa_Completo)) = UPPER(TRIM(source.nombre_Programa_Completo))
# MAGIC    AND target.id_Dim_Programa != -1  -- 🔹 Evitar modificar el registro `-1`
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     COALESCE(TRIM(UPPER(target.nombre_Programa)), '') <> COALESCE(TRIM(UPPER(source.nombre_Programa)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.tipo_Programa)), '') <> COALESCE(TRIM(UPPER(source.tipo_Programa)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.entidad_Legal)), '') <> COALESCE(TRIM(UPPER(source.entidad_Legal)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.especialidad)), '') <> COALESCE(TRIM(UPPER(source.especialidad)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.vertical)), '') <> COALESCE(TRIM(UPPER(source.vertical)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.nombre_Programa_Completo)), '') <> COALESCE(TRIM(UPPER(source.nombre_Programa_Completo)), '') OR
# MAGIC     target.ETLupdatedDate < source.ETLupdatedDate
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.nombre_Programa = source.nombre_Programa,
# MAGIC     target.tipo_Programa = source.tipo_Programa,
# MAGIC     target.entidad_Legal = source.entidad_Legal,
# MAGIC     target.especialidad = source.especialidad,
# MAGIC     target.vertical = source.vertical,
# MAGIC     target.nombre_Programa_Completo = source.nombre_Programa_Completo,
# MAGIC     target.ETLupdatedDate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (cod_Programa, nombre_Programa, tipo_Programa, entidad_Legal, especialidad, vertical, nombre_Programa_Completo, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.cod_Programa, source.nombre_Programa, source.tipo_Programa, source.entidad_Legal, source.especialidad, source.vertical, 
# MAGIC             source.nombre_Programa_Completo, source.ETLcreatedDate, source.ETLupdatedDate);

# COMMAND ----------

# DBTITLE 1,Validate duplicate >1
# MAGIC %sql
# MAGIC SELECT cod_Programa, COUNT(*) AS total_duplicados
# MAGIC FROM gold_lakehouse.dim_programa
# MAGIC GROUP BY cod_Programa
# MAGIC HAVING COUNT(*) > 1;
