# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_VERTICAL**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_vertical_view AS
# MAGIC     SELECT DISTINCT
# MAGIC         vertical AS nombre_Vertical
# MAGIC     FROM gold_lakehouse.dim_producto
# MAGIC     WHERE vertical IS NOT NULL 
# MAGIC     AND vertical != '';
# MAGIC
# MAGIC --select * from dim_vertical_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `idDimVertical = -1` existe solo una vez con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_vertical AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_Vertical, 'n/a' AS nombre_Vertical_Corto
# MAGIC ) AS source
# MAGIC ON UPPER(target.nombre_Vertical) = 'n/a'
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Vertical, nombre_Vertical_Corto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('n/a', 'n/a', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 2️⃣ 🔹 Insertar los valores predeterminados (`FP`, `HE`, `FC`) solo si no existen
# MAGIC MERGE INTO gold_lakehouse.dim_vertical AS target
# MAGIC USING (
# MAGIC     SELECT 'Formación Profesional' AS nombre_Vertical, 'FP' AS nombre_Vertical_Corto UNION ALL
# MAGIC     SELECT 'High Education', 'HE' UNION ALL
# MAGIC     SELECT 'Formación Continua', 'FC'
# MAGIC ) AS source
# MAGIC ON target.nombre_Vertical_Corto = source.nombre_Vertical_Corto
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Vertical, nombre_Vertical_Corto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_Vertical, source.nombre_Vertical_Corto, current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 3️⃣ 🔹 Insertar nuevos valores desde `dim_vertical_view` sin alterar el registro `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_vertical AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT nombre_Vertical FROM dim_vertical_view WHERE nombre_Vertical <> 'n/a'
# MAGIC ) AS source
# MAGIC ON UPPER(target.nombre_Vertical) = UPPER(source.nombre_Vertical)
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Vertical, nombre_Vertical_Corto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (
# MAGIC         source.nombre_Vertical, 
# MAGIC         TRIM(
# MAGIC             CONCAT(
# MAGIC                 UPPER(LEFT(element_at(SPLIT(source.nombre_Vertical, ' '), 1), 1)),  -- Primera letra de la primera palabra
# MAGIC                 UPPER(LEFT(element_at(SPLIT(source.nombre_Vertical, ' '), 2), 1))   -- Primera letra de la segunda palabra
# MAGIC             )
# MAGIC         ), 
# MAGIC         current_timestamp(), 
# MAGIC         current_timestamp()
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3️⃣ 🔹 Asegurar que solo hay un `n/a`
# MAGIC DELETE FROM gold_lakehouse.dim_vertical
# MAGIC WHERE nombre_Vertical = 'n/a' AND id_Dim_Vertical <> -1;
