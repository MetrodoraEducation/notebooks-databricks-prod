# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_ESPECIALIDAD**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_especialidad_view AS
# MAGIC SELECT DISTINCT
# MAGIC     UPPER(TRIM(dp.especialidad)) AS nombre_Especialidad,
# MAGIC     CASE 
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'MEDICINA' THEN 'MD'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'ENFERMERÍA' THEN 'NU'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'PSICOLOGÍA' THEN 'PS'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'FARMACIA' THEN 'PH'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'FISIOTERAPIA' THEN 'PT'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'OPTICA' THEN 'OP'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'BIOLOGÍA' THEN 'BI'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'MISCELANEA' THEN 'MX'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'ODONTOLOGIA' THEN 'OD'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'QUÍMICA' THEN 'CH'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'NUTRICIÓN' THEN 'NT'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'INFORMÁTICA' THEN 'IT'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'DEPORTE' THEN 'SP'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'ADMINISTRACIÓN' THEN 'AD'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'GENERAL' THEN '00'
# MAGIC         ELSE 'n/a'  -- General o sin coincidencia
# MAGIC     END AS cod_Especialidad,
# MAGIC     CURRENT_TIMESTAMP AS ETLcreatedDate,
# MAGIC     CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM gold_lakehouse.dim_producto dp
# MAGIC WHERE dp.especialidad IS NOT NULL AND dp.especialidad <> '';
# MAGIC
# MAGIC select * from dim_especialidad_view

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `idDimEspecialidad = -1` existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_especialidad AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_Especialidad, 'n/a' AS cod_Especialidad
# MAGIC ) AS source
# MAGIC ON target.nombre_Especialidad = 'n/a'
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Especialidad, cod_Especialidad, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('n/a', 'n/a', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 2️⃣ 🔹 MERGE para insertar o actualizar `dim_especialidad`, excluyendo `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_especialidad AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT 
# MAGIC         TRIM(UPPER(nombre_Especialidad)) AS nombre_Especialidad,
# MAGIC         TRIM(cod_Especialidad) AS cod_Especialidad,
# MAGIC         current_timestamp() AS ETLcreatedDate,
# MAGIC         current_timestamp() AS ETLupdatedDate
# MAGIC     FROM dim_especialidad_view
# MAGIC     WHERE nombre_Especialidad IS NOT NULL 
# MAGIC       AND nombre_Especialidad <> '' 
# MAGIC       AND nombre_Especialidad <> 'n/a'  -- Evitar modificar el registro especial
# MAGIC ) AS source
# MAGIC ON UPPER(target.nombre_Especialidad) = source.nombre_Especialidad
# MAGIC
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.cod_Especialidad = source.cod_Especialidad,
# MAGIC         target.ETLupdatedDate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Especialidad, cod_Especialidad, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_Especialidad, source.cod_Especialidad, source.ETLcreatedDate, source.ETLupdatedDate);
