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
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         UPPER(tipo_conversion) AS tipo_conversion 
# MAGIC     FROM 
# MAGIC         tipo_conversion_tablon_view 
# MAGIC     WHERE 
# MAGIC         tipo_conversion IS NOT NULL 
# MAGIC         AND tipo_conversion NOT IN ('', '0') 
# MAGIC     GROUP BY 
# MAGIC         UPPER(tipo_conversion)
# MAGIC ) AS source
# MAGIC ON 
# MAGIC     UPPER(target.tipo_conversion) = UPPER(source.tipo_conversion)
# MAGIC     AND target.id_dim_tipo_conversion != -1
# MAGIC
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.tipo_conversion = source.tipo_conversion,
# MAGIC         target.etlupdateddate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (tipo_conversion, etlcreateddate, etlupdateddate)
# MAGIC     VALUES (source.tipo_conversion, current_timestamp(), current_timestamp())

# COMMAND ----------

#%sql select * from gold_lakehouse.dim_tipo_conversion
