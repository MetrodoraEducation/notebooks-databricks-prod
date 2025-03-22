# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW campania_sales_view
# MAGIC     AS SELECT distinct campania as nombre_campania     
# MAGIC     FROM silver_lakehouse.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_campania AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_campania
# MAGIC ) AS source
# MAGIC ON target.id_dim_campania = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_campania)
# MAGIC     VALUES (source.nombre_campania);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_campania AS target
# MAGIC USING (SELECT UPPER(nombre_campania) AS nombre_campania FROM campania_sales_view GROUP BY UPPER(nombre_campania)) AS source
# MAGIC ON UPPER(target.nombre_campania) = UPPER(source.nombre_campania)
# MAGIC AND target.id_dim_campania != -1  -- 🔹 Evita afectar el registro `-1`
# MAGIC
# MAGIC -- 🔹 **Si ya existe, lo actualiza (si aplicara)**
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.nombre_campania = source.nombre_campania
# MAGIC
# MAGIC -- 🔹 **Si no existe, lo inserta**
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_campania)
# MAGIC     VALUES (source.nombre_campania);
