# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_JERARQUIA_MARKETING**

# COMMAND ----------

# DBTITLE 1,Created View utm_ad_id
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW utm_ad_id AS
# MAGIC SELECT 
# MAGIC       DISTINCT utm_ad_id 
# MAGIC   FROM silver_lakehouse.zohodeals deals
# MAGIC  WHERE deals.utm_ad_id IS NOT NULL AND deals.utm_ad_id <> ''
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC       DISTINCT utm_ad_id 
# MAGIC   FROM silver_lakehouse.zohodeals_38b deals_38b
# MAGIC  WHERE deals_38b.utm_ad_id IS NOT NULL AND deals_38b.utm_ad_id <> '';
# MAGIC
# MAGIC SELECT * FROM utm_ad_id;

# COMMAND ----------

# DBTITLE 1,Merge dim_utm_ad
# MAGIC %sql
# MAGIC -- Paso 1: inserta -1 manualmente si no existe
# MAGIC INSERT INTO gold_lakehouse.dim_utm_ad (utm_ad_id, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 'n/a', null, null
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold_lakehouse.dim_utm_ad WHERE utm_ad_id = 'n/a'
# MAGIC );
# MAGIC
# MAGIC -- Paso 2: MERGE normales (excluye el valor especial 'n/a')
# MAGIC MERGE INTO gold_lakehouse.dim_utm_ad target
# MAGIC USING (
# MAGIC   SELECT utm_ad_id FROM utm_ad_id WHERE utm_ad_id <> 'n/a'
# MAGIC ) source
# MAGIC ON target.utm_ad_id = source.utm_ad_id
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (utm_ad_id, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (source.utm_ad_id, current_timestamp(), current_timestamp());

# COMMAND ----------

# DBTITLE 1,Created View utm_adset_id
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW utm_adset_id AS
# MAGIC SELECT 
# MAGIC       DISTINCT utm_adset_id 
# MAGIC   FROM silver_lakehouse.zohodeals deals
# MAGIC  WHERE deals.utm_adset_id IS NOT NULL AND deals.utm_adset_id <> ''
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC       DISTINCT utm_adset_id 
# MAGIC   FROM silver_lakehouse.zohodeals_38b deals_38b
# MAGIC  WHERE deals_38b.utm_adset_id IS NOT NULL AND deals_38b.utm_adset_id <> '';
# MAGIC
# MAGIC SELECT * FROM utm_adset_id;

# COMMAND ----------

# DBTITLE 1,Merge dim_utm_adset
# MAGIC %sql
# MAGIC -- Paso 1: inserta -1 manualmente si no existe
# MAGIC INSERT INTO gold_lakehouse.dim_utm_adset (utm_adset_id, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 'n/a', null, null
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold_lakehouse.dim_utm_adset WHERE utm_adset_id = 'n/a'
# MAGIC );
# MAGIC
# MAGIC -- Paso 2: MERGE normales (excluye el valor especial 'n/a')
# MAGIC MERGE INTO gold_lakehouse.dim_utm_adset target
# MAGIC USING (
# MAGIC   SELECT utm_adset_id FROM utm_adset_id WHERE utm_adset_id <> 'n/a'
# MAGIC ) source
# MAGIC ON target.utm_adset_id = source.utm_adset_id
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (utm_adset_id, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (source.utm_adset_id, current_timestamp(), current_timestamp());

# COMMAND ----------

# DBTITLE 1,utm_campaign_id
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW utm_campaign_id AS
# MAGIC SELECT 
# MAGIC       DISTINCT utm_campaign_id 
# MAGIC   FROM silver_lakehouse.zohodeals deals
# MAGIC  WHERE deals.utm_campaign_id IS NOT NULL AND deals.utm_campaign_id <> ''
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC       DISTINCT utm_campaign_id 
# MAGIC   FROM silver_lakehouse.zohodeals_38b deals_38b
# MAGIC  WHERE deals_38b.utm_campaign_id IS NOT NULL AND deals_38b.utm_campaign_id <> '';
# MAGIC
# MAGIC SELECT * FROM utm_campaign_id;

# COMMAND ----------

# DBTITLE 1,Merge dim_utm_campaign
# MAGIC %sql
# MAGIC -- Paso 1: inserta -1 manualmente si no existe
# MAGIC INSERT INTO gold_lakehouse.dim_utm_campaign (utm_campaign_id, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 'n/a', null, null
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold_lakehouse.dim_utm_campaign WHERE utm_campaign_id = 'n/a'
# MAGIC );
# MAGIC
# MAGIC -- Paso 2: MERGE normales (excluye el valor especial 'n/a')
# MAGIC MERGE INTO gold_lakehouse.dim_utm_campaign target
# MAGIC USING (
# MAGIC   SELECT utm_campaign_id FROM utm_campaign_id WHERE utm_campaign_id <> 'n/a'
# MAGIC ) source
# MAGIC ON target.utm_campaign_id = source.utm_campaign_id
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (utm_campaign_id, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (source.utm_campaign_id, current_timestamp(), current_timestamp());

# COMMAND ----------

# DBTITLE 1,utm_campaign_name
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW utm_campaign_name AS
# MAGIC SELECT 
# MAGIC       DISTINCT utm_campaign_name 
# MAGIC   FROM silver_lakehouse.zohodeals deals
# MAGIC  WHERE deals.utm_campaign_name IS NOT NULL AND deals.utm_campaign_name <> ''
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC       DISTINCT utm_campaign_name 
# MAGIC   FROM silver_lakehouse.zohodeals_38b deals_38b
# MAGIC  WHERE deals_38b.utm_campaign_name IS NOT NULL AND deals_38b.utm_campaign_name <> '';
# MAGIC
# MAGIC SELECT * FROM utm_campaign_name;

# COMMAND ----------

# DBTITLE 1,Merge dim_utm_campaign_name
# MAGIC %sql
# MAGIC -- Paso 1: inserta -1 manualmente si no existe
# MAGIC INSERT INTO gold_lakehouse.dim_utm_campaign_name (utm_campaign_name, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 'n/a', null, null
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold_lakehouse.dim_utm_campaign_name WHERE utm_campaign_name = 'n/a'
# MAGIC );
# MAGIC
# MAGIC -- Paso 2: MERGE normales (excluye el valor especial 'n/a')
# MAGIC MERGE INTO gold_lakehouse.dim_utm_campaign_name target
# MAGIC USING (
# MAGIC   SELECT utm_campaign_name FROM utm_campaign_name WHERE utm_campaign_name <> 'n/a'
# MAGIC ) source
# MAGIC ON target.utm_campaign_name = source.utm_campaign_name
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (utm_campaign_name, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (source.utm_campaign_name, current_timestamp(), current_timestamp());

# COMMAND ----------

# DBTITLE 1,utm_channel
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW utm_channel AS
# MAGIC SELECT 
# MAGIC       DISTINCT utm_channel 
# MAGIC   FROM silver_lakehouse.zohodeals deals
# MAGIC  WHERE deals.utm_channel IS NOT NULL AND deals.utm_channel <> ''
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC       DISTINCT utm_channel 
# MAGIC   FROM silver_lakehouse.zohodeals_38b deals_38b
# MAGIC  WHERE deals_38b.utm_channel IS NOT NULL AND deals_38b.utm_channel <> '';
# MAGIC
# MAGIC SELECT * FROM utm_channel;

# COMMAND ----------

# DBTITLE 1,Merge dim_utm_channel
# MAGIC %sql
# MAGIC -- Paso 1: inserta -1 manualmente si no existe
# MAGIC INSERT INTO gold_lakehouse.dim_utm_channel (utm_channel, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 'n/a', null, null
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold_lakehouse.dim_utm_channel WHERE utm_channel = 'n/a'
# MAGIC );
# MAGIC
# MAGIC -- Paso 2: MERGE normales (excluye el valor especial 'n/a')
# MAGIC MERGE INTO gold_lakehouse.dim_utm_channel target
# MAGIC USING (
# MAGIC   SELECT utm_channel FROM utm_channel WHERE utm_channel <> 'n/a'
# MAGIC ) source
# MAGIC ON target.utm_channel = source.utm_channel
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (utm_channel, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (source.utm_channel, current_timestamp(), current_timestamp());

# COMMAND ----------

# DBTITLE 1,utm_estrategia
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW utm_estrategia AS
# MAGIC SELECT 
# MAGIC       DISTINCT utm_estrategia 
# MAGIC   FROM silver_lakehouse.zohodeals deals
# MAGIC  WHERE deals.utm_estrategia IS NOT NULL AND deals.utm_estrategia <> ''
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC       DISTINCT utm_estrategia 
# MAGIC   FROM silver_lakehouse.zohodeals_38b deals_38b
# MAGIC  WHERE deals_38b.utm_estrategia IS NOT NULL AND deals_38b.utm_estrategia <> '';
# MAGIC
# MAGIC SELECT * FROM utm_estrategia;

# COMMAND ----------

# DBTITLE 1,Merge dim_utm_estrategia
# MAGIC %sql
# MAGIC -- Paso 1: inserta -1 manualmente si no existe
# MAGIC INSERT INTO gold_lakehouse.dim_utm_estrategia (utm_estrategia, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 'n/a', null, null
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold_lakehouse.dim_utm_estrategia WHERE utm_estrategia = 'n/a'
# MAGIC );
# MAGIC
# MAGIC -- Paso 2: MERGE normales (excluye el valor especial 'n/a')
# MAGIC MERGE INTO gold_lakehouse.dim_utm_estrategia target
# MAGIC USING (
# MAGIC   SELECT utm_estrategia FROM utm_estrategia WHERE utm_estrategia <> 'n/a'
# MAGIC ) source
# MAGIC ON target.utm_estrategia = source.utm_estrategia
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (utm_estrategia, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (source.utm_estrategia, current_timestamp(), current_timestamp());

# COMMAND ----------

# DBTITLE 1,utm_medium
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW utm_medium AS
# MAGIC SELECT 
# MAGIC       DISTINCT utm_medium 
# MAGIC   FROM silver_lakehouse.zohodeals deals
# MAGIC  WHERE deals.utm_medium IS NOT NULL AND deals.utm_medium <> ''
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC       DISTINCT utm_medium 
# MAGIC   FROM silver_lakehouse.zohodeals_38b deals_38b
# MAGIC  WHERE deals_38b.utm_medium IS NOT NULL AND deals_38b.utm_medium <> '';
# MAGIC
# MAGIC SELECT * FROM utm_medium;

# COMMAND ----------

# DBTITLE 1,Merge dim_utm_medium
# MAGIC %sql
# MAGIC -- Paso 1: inserta -1 manualmente si no existe
# MAGIC INSERT INTO gold_lakehouse.dim_utm_medium (utm_medium, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 'n/a', null, null
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold_lakehouse.dim_utm_medium WHERE utm_medium = 'n/a'
# MAGIC );
# MAGIC
# MAGIC -- Paso 2: MERGE normales (excluye el valor especial 'n/a')
# MAGIC MERGE INTO gold_lakehouse.dim_utm_medium target
# MAGIC USING (
# MAGIC   SELECT utm_medium FROM utm_medium WHERE utm_medium <> 'n/a'
# MAGIC ) source
# MAGIC ON target.utm_medium = source.utm_medium
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (utm_medium, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (source.utm_medium, current_timestamp(), current_timestamp());

# COMMAND ----------

# DBTITLE 1,utm_perfil
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW utm_perfil AS
# MAGIC SELECT 
# MAGIC       DISTINCT utm_perfil 
# MAGIC   FROM silver_lakehouse.zohodeals deals
# MAGIC  WHERE deals.utm_perfil IS NOT NULL AND deals.utm_perfil <> ''
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC       DISTINCT utm_perfil 
# MAGIC   FROM silver_lakehouse.zohodeals_38b deals_38b
# MAGIC  WHERE deals_38b.utm_perfil IS NOT NULL AND deals_38b.utm_perfil <> '';
# MAGIC
# MAGIC SELECT * FROM utm_perfil;

# COMMAND ----------

# DBTITLE 1,Merge dim_utm_perfil
# MAGIC %sql
# MAGIC -- Paso 1: inserta -1 manualmente si no existe
# MAGIC INSERT INTO gold_lakehouse.dim_utm_perfil (utm_perfil, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 'n/a', null, null
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold_lakehouse.dim_utm_perfil WHERE utm_perfil = 'n/a'
# MAGIC );
# MAGIC
# MAGIC -- Paso 2: MERGE normales (excluye el valor especial 'n/a')
# MAGIC MERGE INTO gold_lakehouse.dim_utm_perfil target
# MAGIC USING (
# MAGIC   SELECT utm_perfil FROM utm_perfil WHERE utm_perfil <> 'n/a'
# MAGIC ) source
# MAGIC ON target.utm_perfil = source.utm_perfil
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (utm_perfil, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (source.utm_perfil, current_timestamp(), current_timestamp());

# COMMAND ----------

# DBTITLE 1,utm_source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW utm_source AS
# MAGIC SELECT 
# MAGIC       DISTINCT utm_source 
# MAGIC   FROM silver_lakehouse.zohodeals deals
# MAGIC  WHERE deals.utm_source IS NOT NULL AND deals.utm_source <> ''
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC       DISTINCT utm_source 
# MAGIC   FROM silver_lakehouse.zohodeals_38b deals_38b
# MAGIC  WHERE deals_38b.utm_source IS NOT NULL AND deals_38b.utm_source <> '';
# MAGIC
# MAGIC SELECT * FROM utm_source;

# COMMAND ----------

# DBTITLE 1,Merge dim_utm_source
# MAGIC %sql
# MAGIC -- Paso 1: inserta -1 manualmente si no existe
# MAGIC INSERT INTO gold_lakehouse.dim_utm_source (utm_source, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 'n/a', null, null
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold_lakehouse.dim_utm_source WHERE utm_source = 'n/a'
# MAGIC );
# MAGIC
# MAGIC -- Paso 2: MERGE normales (excluye el valor especial 'n/a')
# MAGIC MERGE INTO gold_lakehouse.dim_utm_source target
# MAGIC USING (
# MAGIC   SELECT utm_source FROM utm_source WHERE utm_source <> 'n/a'
# MAGIC ) source
# MAGIC ON target.utm_source = source.utm_source
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (utm_source, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (source.utm_source, current_timestamp(), current_timestamp());

# COMMAND ----------

# DBTITLE 1,utm_term
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW utm_term AS
# MAGIC SELECT 
# MAGIC       DISTINCT utm_term 
# MAGIC   FROM silver_lakehouse.zohodeals deals
# MAGIC  WHERE deals.utm_term IS NOT NULL AND deals.utm_term <> ''
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC       DISTINCT utm_term 
# MAGIC   FROM silver_lakehouse.zohodeals_38b deals_38b
# MAGIC  WHERE deals_38b.utm_term IS NOT NULL AND deals_38b.utm_term <> '';
# MAGIC
# MAGIC SELECT * FROM utm_term;

# COMMAND ----------

# DBTITLE 1,Merge dim_utm_term
# MAGIC %sql
# MAGIC -- Paso 1: inserta -1 manualmente si no existe
# MAGIC INSERT INTO gold_lakehouse.dim_utm_term (utm_term, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 'n/a', null, null
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold_lakehouse.dim_utm_term WHERE utm_term = 'n/a'
# MAGIC );
# MAGIC
# MAGIC -- Paso 2: MERGE normales (excluye el valor especial 'n/a')
# MAGIC MERGE INTO gold_lakehouse.dim_utm_term target
# MAGIC USING (
# MAGIC   SELECT utm_term FROM utm_term WHERE utm_term <> 'n/a'
# MAGIC ) source
# MAGIC ON target.utm_term = source.utm_term
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (utm_term, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (source.utm_term, current_timestamp(), current_timestamp());

# COMMAND ----------

# DBTITLE 1,utm_type
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW utm_type AS
# MAGIC SELECT 
# MAGIC       DISTINCT utm_type 
# MAGIC   FROM silver_lakehouse.zohodeals deals
# MAGIC  WHERE deals.utm_type IS NOT NULL AND deals.utm_type <> ''
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC       DISTINCT utm_type 
# MAGIC   FROM silver_lakehouse.zohodeals_38b deals_38b
# MAGIC  WHERE deals_38b.utm_type IS NOT NULL AND deals_38b.utm_type <> '';
# MAGIC
# MAGIC SELECT * FROM utm_type;

# COMMAND ----------

# DBTITLE 1,Merge dim_utm_type
# MAGIC %sql
# MAGIC -- Paso 1: inserta -1 manualmente si no existe
# MAGIC INSERT INTO gold_lakehouse.dim_utm_type (utm_type, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 'n/a', null, null
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold_lakehouse.dim_utm_type WHERE utm_type = 'n/a'
# MAGIC );
# MAGIC
# MAGIC -- Paso 2: MERGE normales (excluye el valor especial 'n/a')
# MAGIC MERGE INTO gold_lakehouse.dim_utm_type target
# MAGIC USING (
# MAGIC   SELECT utm_type FROM utm_type WHERE utm_type <> 'n/a'
# MAGIC ) source
# MAGIC ON target.utm_type = source.utm_type
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (utm_type, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (source.utm_type, current_timestamp(), current_timestamp());
