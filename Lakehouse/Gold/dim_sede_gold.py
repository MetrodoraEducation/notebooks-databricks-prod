# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_SEDE**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_sede_view AS
# MAGIC SELECT DISTINCT 
# MAGIC     CASE WHEN producto.codigo_sede IS NULL THEN 0
# MAGIC          ELSE producto.codigo_sede
# MAGIC           END number_codigo_sede,
# MAGIC     UPPER(REGEXP_REPLACE(try_element_at(SPLIT(cod_Producto, '-'), 3), '[0-9]', '')) AS codigo_sede,
# MAGIC     CASE WHEN producto.sede IS NULL THEN 'NO REGISTRA'
# MAGIC          ELSE producto.sede
# MAGIC          END nombre_sede
# MAGIC FROM gold_lakehouse.dim_producto producto
# MAGIC WHERE producto.cod_Producto IS NOT NULL
# MAGIC   AND producto.codigo_sede IS NOT NULL
# MAGIC   AND producto.sede <> 'n/a';

# COMMAND ----------

# DBTITLE 1,Merge Into from sales
# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `-1` existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_sede AS target
# MAGIC USING (
# MAGIC     SELECT -1 AS number_codigo_sede, 'n/a' AS nombre_sede, 'n/a' AS codigo_sede
# MAGIC ) AS source
# MAGIC ON target.id_dim_sede = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (number_codigo_sede, nombre_sede, codigo_sede)
# MAGIC     VALUES (-1, 'n/a', 'n/a');
# MAGIC
# MAGIC -- 2️⃣ 🔹 Realizar el MERGE para actualizar o insertar nuevos registros de `dim_sede_view`
# MAGIC MERGE INTO gold_lakehouse.dim_sede AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT number_codigo_sede, nombre_sede, codigo_sede FROM dim_sede_view
# MAGIC ) AS source
# MAGIC ON target.nombre_sede = source.nombre_sede
# MAGIC
# MAGIC -- 🔹 Si el registro ya existe, actualiza su código
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET target.codigo_sede = source.codigo_sede
# MAGIC
# MAGIC -- 🔹 Si el registro no existe, se inserta sin tocar el ID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (number_codigo_sede, nombre_sede, codigo_sede, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.number_codigo_sede, source.nombre_sede, source.codigo_sede, current_timestamp(), current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3️⃣ 🔹 Asegurar que solo haya un único ID `-1`
# MAGIC DELETE FROM gold_lakehouse.dim_sede
# MAGIC WHERE nombre_sede = 'n/a' AND id_dim_sede <> -1;
