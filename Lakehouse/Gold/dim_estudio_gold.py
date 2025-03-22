# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_estudio_view AS 
# MAGIC     SELECT * 
# MAGIC       FROM silver_lakehouse.dim_estudio;
# MAGIC
# MAGIC select * from dim_estudio_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `-1` existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_estudio AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         'n/a' AS cod_estudio,
# MAGIC         'n/a' AS nombre_de_programa,
# MAGIC         'n/a' AS cod_vertical,
# MAGIC         'n/a' AS vertical_desc,
# MAGIC         'n/a' AS cod_entidad_legal,
# MAGIC         'n/a' AS entidad_legal_desc,
# MAGIC         'n/a' AS cod_especialidad,
# MAGIC         'n/a' AS especialidad_desc,
# MAGIC         'n/a' AS cod_tipo_formacion,
# MAGIC         'n/a' AS tipo_formacion_desc,
# MAGIC         'n/a' AS cod_tipo_negocio,
# MAGIC         'n/a' AS tipo_negocio_desc
# MAGIC ) AS source
# MAGIC ON target.id_dim_estudio = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         cod_estudio, nombre_de_programa, cod_vertical, vertical_desc, 
# MAGIC         cod_entidad_legal, entidad_legal_desc, cod_especialidad, especialidad_desc, 
# MAGIC         cod_tipo_formacion, tipo_formacion_desc, cod_tipo_negocio, tipo_negocio_desc
# MAGIC     ) VALUES (
# MAGIC         source.cod_estudio, source.nombre_de_programa, source.cod_vertical, source.vertical_desc, 
# MAGIC         source.cod_entidad_legal, source.entidad_legal_desc, source.cod_especialidad, source.especialidad_desc, 
# MAGIC         source.cod_tipo_formacion, source.tipo_formacion_desc, source.cod_tipo_negocio, source.tipo_negocio_desc
# MAGIC     );
# MAGIC
# MAGIC -- 2️⃣ 🔹 Realizar el MERGE para actualizar o insertar nuevos registros de `dim_estudio_view`
# MAGIC MERGE INTO gold_lakehouse.dim_estudio AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT 
# MAGIC         cod_estudio, nombre_de_programa, cod_vertical, vertical_desc, 
# MAGIC         cod_entidad_legal, entidad_legal_desc, cod_especialidad, especialidad_desc, 
# MAGIC         cod_tipo_formacion, tipo_formacion_desc, cod_tipo_negocio, tipo_negocio_desc
# MAGIC     FROM dim_estudio_view
# MAGIC ) AS source
# MAGIC ON target.cod_estudio = source.cod_estudio
# MAGIC
# MAGIC -- 🔹 Si el registro ya existe, se actualizan los campos
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.nombre_de_programa = source.nombre_de_programa,
# MAGIC         target.cod_vertical = source.cod_vertical,
# MAGIC         target.vertical_desc = source.vertical_desc,
# MAGIC         target.cod_entidad_legal = source.cod_entidad_legal,
# MAGIC         target.entidad_legal_desc = source.entidad_legal_desc,
# MAGIC         target.cod_especialidad = source.cod_especialidad,
# MAGIC         target.especialidad_desc = source.especialidad_desc,
# MAGIC         target.cod_tipo_formacion = source.cod_tipo_formacion,
# MAGIC         target.tipo_formacion_desc = source.tipo_formacion_desc,
# MAGIC         target.cod_tipo_negocio = source.cod_tipo_negocio,
# MAGIC         target.tipo_negocio_desc = source.tipo_negocio_desc
# MAGIC
# MAGIC -- 🔹 Si el registro no existe, se inserta SIN afectar `id_dim_estudio`
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         cod_estudio, nombre_de_programa, cod_vertical, vertical_desc, 
# MAGIC         cod_entidad_legal, entidad_legal_desc, cod_especialidad, especialidad_desc, 
# MAGIC         cod_tipo_formacion, tipo_formacion_desc, cod_tipo_negocio, tipo_negocio_desc
# MAGIC     ) VALUES (
# MAGIC         source.cod_estudio, source.nombre_de_programa, source.cod_vertical, source.vertical_desc, 
# MAGIC         source.cod_entidad_legal, source.entidad_legal_desc, source.cod_especialidad, source.especialidad_desc, 
# MAGIC         source.cod_tipo_formacion, source.tipo_formacion_desc, source.cod_tipo_negocio, source.tipo_negocio_desc
# MAGIC     );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3️⃣ 🔹 Asegurar que solo haya un único ID `-1`
# MAGIC DELETE FROM gold_lakehouse.dim_estudio
# MAGIC WHERE cod_estudio = 'n/a' AND id_dim_estudio <> -1;
