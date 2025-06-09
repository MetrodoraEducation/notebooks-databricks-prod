# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tipo_conversion_tablon_view
# MAGIC     AS SELECT distinct coalesce(tipo_Conversion_opotunidad,tipo_conversion_lead) as tipo_conversion    
# MAGIC     FROM silver_lakehouse.tablon_leads_and_deals;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_conversion AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS tipo_conversion
# MAGIC ) AS source
# MAGIC ON target.id_dim_tipo_conversion = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (tipo_conversion)
# MAGIC     VALUES (source.tipo_conversion);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_conversion AS target
# MAGIC USING (SELECT UPPER(tipo_conversion) AS tipo_conversion FROM tipo_conversion_tablon_view where tipo_conversion is not null and tipo_conversion not in ('','0') GROUP BY UPPER(tipo_conversion)) AS source  -- 🔹 Ojo tratar el espacio vacio y el 0 como nulos.
# MAGIC ON UPPER(target.tipo_conversion) = UPPER(source.tipo_conversion)
# MAGIC AND target.id_dim_tipo_conversion != -1  -- 🔹 Evita afectar el registro `-1`
# MAGIC
# MAGIC -- 🔹 **Si ya existe, lo actualiza (si aplicara)**
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.tipo_conversion = source.tipo_conversion
# MAGIC
# MAGIC -- 🔹 **Si no existe, lo inserta**
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (tipo_conversion)
# MAGIC     VALUES (source.tipo_conversion);
