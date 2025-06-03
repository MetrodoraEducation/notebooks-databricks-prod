# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_PROGRAMA**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_programa_view AS
# MAGIC SELECT 
# MAGIC     cod_Programa,
# MAGIC     nombre_Programa,
# MAGIC     tipo_Programa,
# MAGIC     entidad_Legal,
# MAGIC     especialidad,
# MAGIC     vertical,
# MAGIC     nombre_Programa_Completo,
# MAGIC     MAX(ETLcreatedDate) AS ETLcreatedDate,
# MAGIC     MAX(ETLupdatedDate) AS ETLupdatedDate
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         UPPER(codigo_programa) AS cod_Programa,
# MAGIC         TRIM(UPPER(area_title)) AS nombre_Programa,
# MAGIC         TRIM(UPPER(degree_title)) AS tipo_Programa,
# MAGIC         TRIM(UPPER(group_entidad_legal)) AS entidad_Legal,
# MAGIC         TRIM(UPPER(especialidad)) AS especialidad,
# MAGIC         TRIM(UPPER(group_vertical)) AS vertical,
# MAGIC         TRIM(UPPER(nombre_del_programa_oficial_completo)) AS nombre_Programa_Completo,
# MAGIC         TRY_CAST(fecha_creacion AS TIMESTAMP) AS ETLcreatedDate,
# MAGIC         TRY_CAST(ultima_actualizacion AS TIMESTAMP) AS ETLupdatedDate
# MAGIC     FROM silver_lakehouse.classlifetitulaciones
# MAGIC     WHERE codigo_programa IS NOT NULL AND codigo_programa != ''
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         UPPER(codigo_programa) AS cod_Programa,
# MAGIC         TRIM(UPPER(area_title)) AS nombre_Programa,
# MAGIC         TRIM(UPPER(degree_title)) AS tipo_Programa,
# MAGIC         TRIM(UPPER(group_entidad_legal)) AS entidad_Legal,
# MAGIC         TRIM(UPPER(especialidad)) AS especialidad,
# MAGIC         TRIM(UPPER(group_vertical)) AS vertical,
# MAGIC         TRIM(UPPER(nombre_del_programa_oficial_completo)) AS nombre_Programa_Completo,
# MAGIC         TRY_CAST(fecha_creacion AS TIMESTAMP) AS ETLcreatedDate,
# MAGIC         TRY_CAST(ultima_actualizacion AS TIMESTAMP) AS ETLupdatedDate
# MAGIC     FROM silver_lakehouse.classlifetitulaciones_931
# MAGIC     WHERE codigo_programa IS NOT NULL AND codigo_programa != ''
# MAGIC ) AS union_view
# MAGIC GROUP BY 
# MAGIC     cod_Programa, nombre_Programa, tipo_Programa, entidad_Legal, especialidad, vertical, nombre_Programa_Completo;
# MAGIC
# MAGIC --SELECT * FROM dim_programa_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cod_Programa, entidad_Legal, COUNT(*) AS total
# MAGIC FROM dim_programa_view
# MAGIC GROUP BY cod_Programa, entidad_Legal
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_programa AS target
# MAGIC USING (
# MAGIC     WITH source_deduplicated AS (
# MAGIC         SELECT *, ROW_NUMBER() OVER (
# MAGIC             PARTITION BY cod_Programa                     -- 🔄 Solo cod_Programa
# MAGIC             ORDER BY ETLupdatedDate DESC
# MAGIC         ) AS rn
# MAGIC         FROM dim_programa_view 
# MAGIC         WHERE cod_Programa <> 'n/a'
# MAGIC     )
# MAGIC     SELECT cod_Programa, nombre_Programa, tipo_Programa, entidad_Legal, especialidad, vertical, nombre_Programa_Completo, ETLcreatedDate, ETLupdatedDate
# MAGIC     FROM source_deduplicated
# MAGIC     WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON UPPER(TRIM(target.cod_Programa)) = UPPER(TRIM(source.cod_Programa))
# MAGIC AND UPPER(TRIM(target.entidad_Legal)) = UPPER(TRIM(source.entidad_Legal))
# MAGIC    AND target.id_Dim_Programa != -1
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     TRIM(UPPER(target.nombre_Programa)) IS DISTINCT FROM TRIM(UPPER(source.nombre_Programa)) OR
# MAGIC     TRIM(UPPER(target.tipo_Programa)) IS DISTINCT FROM TRIM(UPPER(source.tipo_Programa)) OR
# MAGIC     TRIM(UPPER(target.entidad_Legal)) IS DISTINCT FROM TRIM(UPPER(source.entidad_Legal)) OR
# MAGIC     TRIM(UPPER(target.especialidad)) IS DISTINCT FROM TRIM(UPPER(source.especialidad)) OR
# MAGIC     TRIM(UPPER(target.vertical)) IS DISTINCT FROM TRIM(UPPER(source.vertical)) OR
# MAGIC     TRIM(UPPER(target.nombre_Programa_Completo)) IS DISTINCT FROM TRIM(UPPER(source.nombre_Programa_Completo)) OR
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
# MAGIC SELECT cod_Programa, entidad_Legal, COUNT(*) AS total_duplicados
# MAGIC FROM gold_lakehouse.dim_programa
# MAGIC GROUP BY cod_Programa, entidad_Legal
# MAGIC HAVING COUNT(*) > 1;
