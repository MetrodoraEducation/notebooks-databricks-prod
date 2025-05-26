# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_TIPO_FORMACION**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tipo_formacion_view AS 
# MAGIC     SELECT 
# MAGIC         DISTINCT tipo_formacion_desc AS tipo_formacion_desc,
# MAGIC         cod_tipo_formacion	AS cod_tipo_formacion
# MAGIC     FROM gold_lakehouse.dim_estudio;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `-1` existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_formacion AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS tipo_formacion_desc, 'n/a' AS cod_tipo_formacion
# MAGIC ) AS source
# MAGIC ON target.id_dim_tipo_formacion = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (tipo_formacion_desc, cod_tipo_formacion)
# MAGIC     VALUES ('n/a', 'n/a');
# MAGIC
# MAGIC -- 2️⃣ 🔹 Realizar el MERGE para actualizar o insertar nuevos registros de `tipo_formacion_view`
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_formacion AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT tipo_formacion_desc, cod_tipo_formacion FROM tipo_formacion_view
# MAGIC ) AS source
# MAGIC ON target.tipo_formacion_desc = source.tipo_formacion_desc
# MAGIC
# MAGIC -- 🔹 Si el registro ya existe, actualiza su código
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET target.cod_tipo_formacion = source.cod_tipo_formacion
# MAGIC
# MAGIC -- 🔹 Si el registro no existe, se inserta sin tocar el ID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (tipo_formacion_desc, cod_tipo_formacion, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.tipo_formacion_desc, source.cod_tipo_formacion, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3️⃣ 🔹 Asegurar que solo haya un único ID `-1`
# MAGIC DELETE FROM gold_lakehouse.dim_tipo_formacion
# MAGIC WHERE tipo_formacion_desc = 'n/a' AND id_dim_tipo_formacion <> -1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_tipo_formacion_view AS
# MAGIC SELECT DISTINCT
# MAGIC     UPPER(TRIM(dp.tipo_Producto)) AS nombre_Tipo_Formacion,
# MAGIC     CASE 
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%ACCESO A ESPECIALIDAD' THEN '01'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%MÁSTER' THEN '02'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%CURSO POSTGRADO' THEN '03'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%CURSO EXPERTO' THEN '04'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%TALLER/SEMINARIO' THEN '05'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%PREGRADO' THEN '06'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%OPOSICIÓN' THEN '07'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%OTROS CURSOS' THEN '08'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%FORMACIÓN CONTINUA' THEN '09'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%EXECUTIVE EDUCATION' THEN '10'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%PROGRAMA DESARROLLO DIRECTIVO (PDD)' THEN '11'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%CFGS' THEN '12'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%CFGM' THEN '13'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%GRADO' THEN '14'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%CERTIFICADO PROFESIONALIDAD' THEN '15'
# MAGIC         WHEN UPPER(TRIM(dp.tipo_Producto)) LIKE '%CURSO ESPECIALIZACIÓN' THEN '16'
# MAGIC         ELSE '00' -- GENERAL o valores no contemplados
# MAGIC     END AS cod_Tipo_Formacion,
# MAGIC     CURRENT_TIMESTAMP AS ETLcreatedDate,
# MAGIC     CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM gold_lakehouse.dim_producto dp
# MAGIC WHERE dp.tipo_Producto IS NOT NULL AND dp.tipo_Producto <> '';

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_formacion AS target
# MAGIC USING dim_tipo_formacion_view AS source
# MAGIC ON UPPER(target.tipo_formacion_desc) = UPPER(source.nombre_Tipo_Formacion) -- Cambio aquí
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.cod_tipo_formacion = source.cod_Tipo_Formacion,  -- Cambio aquí
# MAGIC         target.ETLupdatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (tipo_formacion_desc, cod_tipo_formacion, ETLcreatedDate, ETLupdatedDate) -- Cambio aquí
# MAGIC     VALUES (source.nombre_Tipo_Formacion, source.cod_Tipo_Formacion, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
