# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_ESTUDIANTE**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_estudiante_view AS
# MAGIC     SELECT
# MAGIC           origen.id_Dim_Origen_SIS AS id_origen_sis
# MAGIC          ,CONCAT(origen.codigo_Origen_SIS, students.student_id) AS cod_estudiante 
# MAGIC          ,students.student_full_name AS nombre_estudiante
# MAGIC          ,students.student_email AS email
# MAGIC          ,students.student_phone AS phone
# MAGIC          ,TRY_CAST(students.student_registration_date AS TIMESTAMP) AS fecha_creacion
# MAGIC          ,students.student_active AS estado
# MAGIC          ,CASE
# MAGIC             WHEN students.nacimiento = '-' THEN NULL
# MAGIC             WHEN students.nacimiento LIKE '__/__/____' OR students.nacimiento LIKE '_/_/____' OR students.nacimiento LIKE '__/_/____' THEN TO_DATE(students.nacimiento, 'd/M/yyyy')
# MAGIC             WHEN students.nacimiento LIKE '____-__-__ __:__:__' THEN TO_DATE(students.nacimiento, 'yyyy-MM-dd HH:mm:ss')
# MAGIC             WHEN students.nacimiento LIKE '____-__-__' THEN TO_DATE(students.nacimiento, 'yyyy-MM-dd')
# MAGIC             ELSE NULL
# MAGIC           END AS edad
# MAGIC          ,students.zoho_id AS id_zoho
# MAGIC          ,students.pais AS pais
# MAGIC          ,students.ciudad AS ciudad
# MAGIC          ,students.codigo AS codigo_postal
# MAGIC          ,students.direccion AS direccion_postal
# MAGIC     FROM silver_lakehouse.classlifeStudents students
# MAGIC LEFT JOIN gold_lakehouse.origenClasslife origen 
# MAGIC       ON 1 = origen.id_Dim_Origen_SIS
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC       origen.id_Dim_Origen_SIS AS id_origen_sis
# MAGIC       ,CONCAT(origen.codigo_Origen_SIS, students.student_id) AS cod_estudiante
# MAGIC       ,students.student_full_name AS nombre_estudiante
# MAGIC       ,students.student_email AS email
# MAGIC       ,students.student_phone AS phone
# MAGIC       ,TRY_CAST(students.student_registration_date AS TIMESTAMP) AS fecha_creacion
# MAGIC       ,students.student_active AS estado
# MAGIC       ,CASE
# MAGIC             WHEN students.nacimiento = '-' THEN NULL
# MAGIC             WHEN students.nacimiento LIKE '__/__/____' OR students.nacimiento LIKE '_/_/____' OR students.nacimiento LIKE '__/_/____' THEN TO_DATE(students.nacimiento, 'd/M/yyyy')
# MAGIC             WHEN students.nacimiento LIKE '____-__-__ __:__:__' THEN TO_DATE(students.nacimiento, 'yyyy-MM-dd HH:mm:ss')
# MAGIC             WHEN students.nacimiento LIKE '____-__-__' THEN TO_DATE(students.nacimiento, 'yyyy-MM-dd')
# MAGIC       ELSE NULL
# MAGIC       END AS edad
# MAGIC       ,students.zoho_id AS id_zoho
# MAGIC       ,students.pais AS pais
# MAGIC       ,students.ciudad AS ciudad
# MAGIC       ,students.codigo AS codigo_postal
# MAGIC       ,students.direccion AS direccion_postal
# MAGIC FROM silver_lakehouse.ClasslifeStudents_931 students
# MAGIC LEFT JOIN gold_lakehouse.origenClasslife origen 
# MAGIC   ON 2 = origen.id_Dim_Origen_SIS;
# MAGIC
# MAGIC --select count(*) from dim_estudiante_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `id_dim_estudiante = -1` existe solo una vez con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_estudiante AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         -1 AS id_origen_sis,'-1' AS cod_estudiante,'n/a' AS nombre_estudiante,'n/a' AS email,'n/a' AS phone,NULL AS fecha_creacion,'n/a' AS estado, '1900-01-01' AS edad,'n/a' AS id_zoho,'n/a' AS pais,'n/a' AS ciudad,'n/a' AS codigo_postal,'n/a' AS direccion_postal
# MAGIC ) AS source
# MAGIC ON target.id_origen_sis = -1 AND target.cod_estudiante = '-1'
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         id_origen_sis, cod_estudiante, nombre_estudiante, email, phone, fecha_creacion, estado, edad, 
# MAGIC         id_zoho, pais, ciudad, codigo_postal, direccion_postal, ETLcreatedDate, ETLupdatedDate
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         -1, '-1', 'n/a', 'n/a', 'n/a', NULL, 'n/a','1900-01-01', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a',
# MAGIC         current_timestamp(), current_timestamp()
# MAGIC     );
# MAGIC
# MAGIC -- 2️⃣ 🔹 Insertar nuevos valores desde `dim_estudiante_view` sin duplicar registros ni alterar el registro `-1`
# MAGIC MERGE INTO gold_lakehouse.dim_estudiante AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT
# MAGIC         id_origen_sis, 
# MAGIC         cod_estudiante, 
# MAGIC         nombre_estudiante AS nombre_estudiante, 
# MAGIC         email AS email, 
# MAGIC         phone AS phone, 
# MAGIC         fecha_creacion, 
# MAGIC         estado AS estado, 
# MAGIC         edad AS edad, 
# MAGIC         id_zoho AS id_zoho, 
# MAGIC         pais AS pais, 
# MAGIC         ciudad AS ciudad, 
# MAGIC         codigo_postal AS codigo_postal, 
# MAGIC         direccion_postal AS direccion_postal
# MAGIC     FROM dim_estudiante_view
# MAGIC     WHERE cod_estudiante <> '-1'
# MAGIC ) AS source
# MAGIC ON target.id_origen_sis = source.id_origen_sis 
# MAGIC AND target.cod_estudiante = source.cod_estudiante
# MAGIC
# MAGIC -- 🔄 Actualizar si hay cambios en los datos
# MAGIC WHEN MATCHED AND (
# MAGIC     target.nombre_estudiante IS DISTINCT FROM source.nombre_estudiante
# MAGIC     OR target.email IS DISTINCT FROM source.email
# MAGIC     OR target.phone IS DISTINCT FROM source.phone
# MAGIC     OR target.fecha_creacion IS DISTINCT FROM source.fecha_creacion
# MAGIC     OR target.estado IS DISTINCT FROM source.estado
# MAGIC     OR target.edad IS DISTINCT FROM source.edad
# MAGIC     OR target.id_zoho IS DISTINCT FROM source.id_zoho
# MAGIC     OR target.pais IS DISTINCT FROM source.pais
# MAGIC     OR target.ciudad IS DISTINCT FROM source.ciudad
# MAGIC     OR target.codigo_postal IS DISTINCT FROM source.codigo_postal
# MAGIC     OR target.direccion_postal IS DISTINCT FROM source.direccion_postal
# MAGIC     ) 
# MAGIC THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.nombre_estudiante = source.nombre_estudiante,
# MAGIC         target.email = source.email,
# MAGIC         target.phone = source.phone,
# MAGIC         target.fecha_creacion = source.fecha_creacion,
# MAGIC         target.estado = source.estado,
# MAGIC         target.edad = source.edad,
# MAGIC         target.id_zoho = source.id_zoho,
# MAGIC         target.pais = source.pais,
# MAGIC         target.ciudad = source.ciudad,
# MAGIC         target.codigo_postal = source.codigo_postal,
# MAGIC         target.direccion_postal = source.direccion_postal,
# MAGIC         target.ETLupdatedDate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         id_origen_sis, cod_estudiante, nombre_estudiante, email, phone, fecha_creacion, estado, edad, 
# MAGIC         id_zoho, pais, ciudad, codigo_postal, direccion_postal, ETLcreatedDate, ETLupdatedDate
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.id_origen_sis, source.cod_estudiante, source.nombre_estudiante, source.email, source.phone, 
# MAGIC         source.fecha_creacion, source.estado, source.edad, source.id_zoho, source.pais, source.ciudad, 
# MAGIC         source.codigo_postal, source.direccion_postal, current_timestamp(), current_timestamp()
# MAGIC     );
# MAGIC

# COMMAND ----------

#%sql select * from gold_lakehouse.dim_estudiante;
