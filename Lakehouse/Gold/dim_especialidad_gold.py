# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_ESPECIALIDAD**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_especialidad_view AS
# MAGIC SELECT DISTINCT
# MAGIC     CASE 
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'MEDICINA' THEN 'MEDICINA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('ENFERMERÍA','ENFERMERIA') THEN 'ENFERMERÍA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('PSICOLOGÍA', 'PSICOLOGIA') THEN 'PSICOLOGÍA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'FARMACIA' THEN 'FARMACIA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'FISIOTERAPIA' THEN 'FISIOTERAPIA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('OPTICA','ÓPTICA') THEN 'ÓPTICA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('BIOLOGÍA','BIOLOGIA') THEN 'BIOLOGÍA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('MISCELANEA','MISCELÁNEA') THEN 'MISCELANEA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('ODONTOLOGIA','ODONTOLOGÍA') THEN 'ODONTOLOGÍA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('QUÍMICA','QUIMICA') THEN 'QUÍMICA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('NUTRICIÓN','NUTRICION') THEN 'NUTRICIÓN'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('INFORMÁTICA', 'INFORMATICA', 'TECNOLOGÍA','TECNOLOGIA') THEN 'INFORMÁTICA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('DEPORTE','DEPORTES') THEN 'DEPORTE'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'SANIDAD' THEN 'SANIDAD'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('ADMINISTRACIÓN','ADMINISTRACION') THEN 'ADMINISTRACIÓN'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'EMPRESA' THEN 'EMPRESA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'SOCIAL' THEN 'SOCIAL'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'GENERAL' THEN 'GENERAL'
# MAGIC         ELSE UPPER(TRIM(dp.especialidad)) 
# MAGIC     END AS nombre_Especialidad,
# MAGIC     CASE 
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'MEDICINA' THEN 'MD'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('ENFERMERÍA','ENFERMERIA') THEN 'NU'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('PSICOLOGÍA', 'PSICOLOGIA') THEN 'PS'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'FARMACIA' THEN 'PH'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'FISIOTERAPIA' THEN 'PT'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('OPTICA','ÓPTICA') THEN 'OP'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('BIOLOGÍA','BIOLOGIA') THEN 'BI'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('MISCELANEA','MISCELÁNEA') THEN 'MX'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('ODONTOLOGIA','ODONTOLOGÍA') THEN 'OD'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('QUÍMICA','QUIMICA') THEN 'CH'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('NUTRICIÓN','NUTRICION') THEN 'NT'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('INFORMÁTICA', 'INFORMATICA', 'TECNOLOGÍA','TECNOLOGIA') THEN 'IT'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('DEPORTE','DEPORTES') THEN 'SP'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) in ('ADMINISTRACIÓN','ADMINISTRACION') THEN 'AD'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'SANIDAD' THEN 'SA'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'EMPRESA' THEN 'BS'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'SOCIAL' THEN 'SO'
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
