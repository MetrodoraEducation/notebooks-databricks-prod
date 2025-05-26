# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_TIPO_NEGOCIO**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tipo_negocio_view AS 
# MAGIC     SELECT 
# MAGIC         DISTINCT tipo_negocio_desc AS tipo_negocio_desc,
# MAGIC         cod_tipo_negocio	AS cod_tipo_negocio
# MAGIC     FROM gold_lakehouse.dim_estudio;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `-1` existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_negocio AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS tipo_negocio_desc, 'n/a' AS cod_tipo_negocio
# MAGIC ) AS source
# MAGIC ON target.id_dim_tipo_negocio = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (tipo_negocio_desc, cod_tipo_negocio)
# MAGIC     VALUES ('n/a', 'n/a');
# MAGIC
# MAGIC -- 2️⃣ 🔹 Realizar el MERGE para actualizar o insertar nuevos registros de `tipo_negocio_view`
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_negocio AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT tipo_negocio_desc, cod_tipo_negocio FROM tipo_negocio_view
# MAGIC ) AS source
# MAGIC ON target.tipo_negocio_desc = source.tipo_negocio_desc
# MAGIC
# MAGIC -- 🔹 Si el registro ya existe, actualiza su código
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET target.cod_tipo_negocio = source.cod_tipo_negocio
# MAGIC
# MAGIC -- 🔹 Si el registro no existe, se inserta sin tocar el ID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (tipo_negocio_desc, cod_tipo_negocio, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.tipo_negocio_desc, source.cod_tipo_negocio, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3️⃣ 🔹 Asegurar que solo haya un único ID `-1`
# MAGIC DELETE FROM gold_lakehouse.dim_tipo_negocio
# MAGIC WHERE tipo_negocio_desc = 'n/a' AND id_dim_tipo_negocio <> -1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_tipo_negocio_view AS
# MAGIC SELECT DISTINCT
# MAGIC     dp.tipo_Negocio AS nombre_Tipo_Negocio,
# MAGIC     CASE 
# MAGIC         WHEN UPPER(dp.tipo_Negocio) = 'B2C' THEN 'C'
# MAGIC         WHEN UPPER(dp.tipo_Negocio) = 'B2B' THEN 'B'
# MAGIC         ELSE 'N/A' -- Si hay valores inesperados
# MAGIC     END AS codigo,
# MAGIC     CURRENT_TIMESTAMP AS ETLcreatedDate,
# MAGIC     CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM gold_lakehouse.dim_producto dp
# MAGIC WHERE dp.tipo_Negocio IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_negocio AS target
# MAGIC USING dim_tipo_negocio_view AS source
# MAGIC ON UPPER(target.tipo_negocio_desc) = UPPER(source.nombre_Tipo_Negocio)
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.ETLupdatedDate = CURRENT_TIMESTAMP,
# MAGIC         target.cod_tipo_negocio = source.codigo,
# MAGIC         target.ETLcreatedDate = COALESCE(target.ETLcreatedDate, source.ETLcreatedDate)  -- Mantiene ETLcreatedDate
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (tipo_negocio_desc, cod_tipo_negocio, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_Tipo_Negocio, source.codigo, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
