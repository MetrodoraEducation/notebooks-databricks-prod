# Databricks notebook source
# DBTITLE 1,Create View
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tablon_leads_and_deals AS
# MAGIC                 SELECT  
# MAGIC                         CASE 
# MAGIC                                 WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN 1
# MAGIC                                 WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN 2
# MAGIC                                 WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN 3
# MAGIC                                 ELSE -1
# MAGIC                         END AS id_tipo_registro,
# MAGIC                         CASE 
# MAGIC                                 WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN 'Con lead sin oportunidad'
# MAGIC                                 WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN 'Con lead y con oportunidad'
# MAGIC                                 WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN 'Oportunidad sin lead'
# MAGIC                                 ELSE 'n/a'
# MAGIC                         END AS tipo_registro,
# MAGIC                         leads.id AS cod_Lead,
# MAGIC                         leads.Description AS lead_Nombre,
# MAGIC                         leads.First_Name AS Nombre,
# MAGIC                         leads.Last_Name AS Apellido1,
# MAGIC                         leads.Apellido_2 AS Apellido2,
# MAGIC                         leads.Email AS email,
# MAGIC                         leads.Mobile AS telefono1,
# MAGIC                         COALESCE(leads.Nacionalidad, deals.Nacionalidad1) AS nacionalidad,
# MAGIC                         leads.Phone AS telefono2,
# MAGIC                         leads.Provincia AS provincia,
# MAGIC                         COALESCE(leads.Residencia, deals.Residencia1) AS residencia,
# MAGIC                         leads.Sexo AS sexo,
# MAGIC                         COALESCE(leads.lead_rating, deals.br_rating) AS lead_Rating,
# MAGIC                         COALESCE(try_cast(leads.lead_scoring AS DOUBLE), try_cast(deals.br_score AS DOUBLE)) AS leadScoring,
# MAGIC                         COALESCE(leads.Lead_Status, deals.etapa) AS etapa,
# MAGIC                         COALESCE(leads.Motivos_perdida, deals.Motivo_perdida_B2B, deals.Motivo_perdida_B2C) AS motivo_Perdida,
# MAGIC                         deals.Probabilidad AS probabilidad_Conversion,
# MAGIC                         deals.Pipeline AS flujo_Venta,
# MAGIC                         deals.Profesion_Estudiante AS profesion_Estudiante,
# MAGIC                         deals.Competencia AS competencia,
# MAGIC                         COALESCE(leads.Tipologia_cliente, deals.Tipologia_cliente) AS tipo_Cliente_lead,
# MAGIC                         leads.tipo_conversion as tipo_conversion_lead,
# MAGIC                         COALESCE(leads.utm_ad_id, deals.utm_ad_id) AS utm_ad_id,
# MAGIC                         COALESCE(leads.utm_adset_id, deals.utm_adset_id) AS utm_adset_id,
# MAGIC                         COALESCE(leads.utm_campaign_id, deals.utm_campaign_id) AS utm_campaign_id,
# MAGIC                         COALESCE(leads.utm_campaign_name, deals.utm_campaign_name) AS utm_campaign_name,
# MAGIC                         COALESCE(leads.utm_channel, deals.utm_channel) AS utm_channel,
# MAGIC                         COALESCE(leads.utm_strategy, deals.utm_strategy) AS utm_estrategia,
# MAGIC                         COALESCE(leads.utm_medium, deals.utm_medium) AS utm_medium,
# MAGIC                         COALESCE(leads.utm_profile, deals.utm_profile) AS utm_perfil,
# MAGIC                         COALESCE(leads.utm_source, deals.utm_source) AS utm_source,
# MAGIC                         COALESCE(leads.utm_term, deals.utm_term) AS utm_term,
# MAGIC                         COALESCE(leads.utm_type, deals.utm_type) AS utm_type,
# MAGIC                         COALESCE(leads.Owner_id, deals.Owner_id) AS cod_Owner,
# MAGIC                         COALESCE(leads.id_producto, deals.ID_Producto) AS cod_Producto,
# MAGIC                         COALESCE(leads.lead_correlation_id, deals.lead_correlation_id) AS lead_Correlation,
# MAGIC                         leads.Created_Time AS fecha_Creacion_Lead, --leads.Created_Time
# MAGIC                         leads.Modified_Time AS fecha_Modificacion_Lead,
# MAGIC                         CASE WHEN deals.etapa = 'Perdido' THEN 'PERDIDA'
# MAGIC                             WHEN deals.etapa = 'Matriculado' OR deals.etapa = 'NEC' THEN 'GANADA'
# MAGIC                             ELSE 'ABIERTA'
# MAGIC                         END AS nombre_estado_venta,
# MAGIC                         deals.id AS cod_Oportunidad,
# MAGIC                         deals.ID_Classlife AS cod_Classlife,
# MAGIC                         deals.Deal_Name AS nombre_Oportunidad,
# MAGIC                         deals.contact_name_id AS cod_Contacto,
# MAGIC                         deals.fecha_Cierre AS fecha_Cierre,
# MAGIC                         deals.id_unico AS cod_Unico_Zoho,
# MAGIC                         deals.Exchange_Rate AS ratio_Moneda,
# MAGIC                         deals.Currency AS moneda,
# MAGIC                         deals.Importe_pagado AS importe_Pagado,
# MAGIC                         deals.Codigo_descuento AS cod_Descuento,
# MAGIC                         deals.Descuento AS pct_Descuento,
# MAGIC                         deals.importe AS importe,
# MAGIC                         deals.Tipologia_alumno1 AS tipo_Alumno,
# MAGIC                         deals.tipo_conversion AS tipo_Conversion_opotunidad,
# MAGIC                         deals.Tipologia_cliente AS tipo_Cliente_oportunidad,
# MAGIC                         deals.fecha_hora_Pagado as fecha_hora_Pagado,
# MAGIC                         deals.Created_Time AS fecha_Creacion_Oportunidad, --deals.Created_Time
# MAGIC                         deals.Modified_Time AS fecha_Modificacion_Oportunidad,
# MAGIC                         deals.fecha_hora_anulacion as fecha_hora_Anulacion,
# MAGIC                         deals.id_classlife as id_classlife,
# MAGIC                         CASE 
# MAGIC                             WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.processdate
# MAGIC                             WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN COALESCE(deals.processdate, leads.processdate)
# MAGIC                             WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN deals.processdate
# MAGIC                         ELSE NULL
# MAGIC                         END AS processdate,
# MAGIC                         CASE 
# MAGIC                             WHEN leads.id IS NOT NULL AND deals.id_lead IS NULL THEN leads.sourcesystem
# MAGIC                             WHEN leads.id IS NOT NULL AND deals.id_lead IS NOT NULL THEN COALESCE(deals.sourcesystem, leads.sourcesystem)
# MAGIC                             WHEN leads.id IS NULL AND deals.id_lead IS NOT NULL THEN deals.sourcesystem
# MAGIC                         ELSE NULL
# MAGIC                         END AS sourcesystem
# MAGIC                 FROM silver_lakehouse.zoholeads leads
# MAGIC      FULL OUTER JOIN silver_lakehouse.zohodeals deals
# MAGIC                   ON leads.id = deals.id_lead;
# MAGIC
# MAGIC SELECT * FROM tablon_leads_and_deals;

# COMMAND ----------

# DBTITLE 1,View Row Number
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW staging_tablon AS
# MAGIC SELECT * FROM (
# MAGIC     SELECT *, 
# MAGIC            ROW_NUMBER() OVER (
# MAGIC                PARTITION BY COALESCE(cod_Lead, ''), COALESCE(cod_Oportunidad, '') 
# MAGIC                ORDER BY processdate DESC
# MAGIC            ) AS rn
# MAGIC     FROM tablon_leads_and_deals
# MAGIC ) filtered
# MAGIC WHERE rn = 1;  -- 🔹 Solo conserva la versión más reciente

# COMMAND ----------

# DBTITLE 1,Estado 1 → Estado 2
# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.tablon_leads_and_deals AS target
# MAGIC USING staging_tablon AS source
# MAGIC ON 
# MAGIC     COALESCE(target.cod_Lead, '') = COALESCE(source.cod_Lead, '') 
# MAGIC     AND target.cod_Oportunidad IS NULL 
# MAGIC     AND source.cod_Oportunidad IS NOT NULL
# MAGIC
# MAGIC WHEN MATCHED AND target.fecha_Modificacion_Oportunidad < source.fecha_Modificacion_Oportunidad THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.cod_Oportunidad = source.cod_Oportunidad,
# MAGIC         target.id_tipo_registro = 2,
# MAGIC         target.tipo_registro = 'Con lead y con oportunidad',
# MAGIC         target.fecha_Modificacion_Oportunidad = source.fecha_Modificacion_Oportunidad,
# MAGIC         target.processdate = source.processdate,
# MAGIC         target.sourcesystem = source.sourcesystem;

# COMMAND ----------

# DBTITLE 1,Estado 3 → Estado 2
# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.tablon_leads_and_deals AS target
# MAGIC USING staging_tablon AS source
# MAGIC ON 
# MAGIC     COALESCE(target.cod_Oportunidad, '') = COALESCE(source.cod_Oportunidad, '') 
# MAGIC     AND target.cod_Lead IS NULL 
# MAGIC     AND source.cod_Lead IS NOT NULL
# MAGIC
# MAGIC WHEN MATCHED AND target.fecha_Modificacion_Lead < source.fecha_Modificacion_Lead THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.cod_Lead = source.cod_Lead,
# MAGIC         target.id_tipo_registro = 2,
# MAGIC         target.tipo_registro = 'Con lead y con oportunidad',
# MAGIC         target.fecha_Modificacion_Lead = source.fecha_Modificacion_Lead,
# MAGIC         target.processdate = source.processdate,
# MAGIC         target.sourcesystem = source.sourcesystem;

# COMMAND ----------

# DBTITLE 1,Insertar Nuevos Registros si No Existen
# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.tablon_leads_and_deals AS target
# MAGIC USING staging_tablon AS source
# MAGIC ON 
# MAGIC     COALESCE(target.cod_Lead, '') = COALESCE(source.cod_Lead, '') 
# MAGIC     AND COALESCE(target.cod_Oportunidad, '') = COALESCE(source.cod_Oportunidad, '')
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         id_tipo_registro, tipo_registro, cod_Lead, cod_Oportunidad, lead_Nombre, Nombre, Apellido1, Apellido2,
# MAGIC         email, telefono1, nacionalidad, telefono2, provincia, residencia, sexo, lead_Rating, 
# MAGIC         leadScoring, etapa, motivo_Perdida, probabilidad_Conversion, flujo_Venta, 
# MAGIC         profesion_Estudiante, competencia, tipo_Cliente_lead, tipo_conversion_lead, 
# MAGIC         utm_ad_id, utm_adset_id, utm_campaign_id, utm_campaign_name, utm_channel, utm_estrategia, 
# MAGIC         utm_medium, utm_perfil, utm_source, utm_term, utm_type, cod_Owner, cod_Producto, 
# MAGIC         lead_Correlation, fecha_Creacion_Lead, fecha_Modificacion_Lead, nombre_estado_venta, 
# MAGIC         fecha_Cierre, cod_Unico_Zoho, ratio_Moneda, moneda, importe_Pagado, 
# MAGIC         cod_Descuento, pct_Descuento, importe, tipo_Alumno, 
# MAGIC         tipo_Conversion_opotunidad, tipo_Cliente_oportunidad, id_classlife, fecha_hora_Pagado, fecha_Creacion_Oportunidad, 
# MAGIC         fecha_Modificacion_Oportunidad, fecha_hora_anulacion, processdate, sourcesystem
# MAGIC     ) 
# MAGIC     VALUES (
# MAGIC         source.id_tipo_registro, source.tipo_registro,source.cod_Lead, source.cod_Oportunidad, source.lead_Nombre, source.Nombre, 
# MAGIC         source.Apellido1, source.Apellido2, source.email, source.telefono1, source.nacionalidad, source.telefono2, 
# MAGIC         source.provincia, source.residencia, source.sexo, source.lead_Rating, source.leadScoring, source.etapa, 
# MAGIC         source.motivo_Perdida, source.probabilidad_Conversion, source.flujo_Venta, source.profesion_Estudiante, 
# MAGIC         source.competencia, source.tipo_Cliente_lead, source.tipo_conversion_lead, source.utm_ad_id, source.utm_adset_id, 
# MAGIC         source.utm_campaign_id, source.utm_campaign_name, source.utm_channel, source.utm_estrategia, source.utm_medium, 
# MAGIC         source.utm_perfil, source.utm_source, source.utm_term, source.utm_type, source.cod_Owner, source.cod_Producto, 
# MAGIC         source.lead_Correlation, source.fecha_Creacion_Lead, source.fecha_Modificacion_Lead, source.nombre_estado_venta, 
# MAGIC         source.fecha_Cierre, source.cod_Unico_Zoho, source.ratio_Moneda, source.moneda, source.importe_Pagado, 
# MAGIC         source.cod_Descuento, source.pct_Descuento, source.importe, source.tipo_Alumno, source.tipo_Conversion_opotunidad, 
# MAGIC         source.tipo_Cliente_oportunidad, source.id_classlife ,source.fecha_hora_Pagado, source.fecha_Creacion_Oportunidad, source.fecha_Modificacion_Oportunidad, 
# MAGIC         source.fecha_hora_anulacion, source.processdate, source.sourcesystem
# MAGIC     );

# COMMAND ----------

# DBTITLE 1,Eliminar Registros Duplicados de Estados Anteriores
# MAGIC %sql
# MAGIC DELETE FROM silver_lakehouse.tablon_leads_and_deals
# MAGIC WHERE id_tipo_registro IN (1,3)
# MAGIC AND (
# MAGIC     -- Si existe un estado 2 con la misma `cod_Lead`
# MAGIC     EXISTS (
# MAGIC         SELECT 1 FROM silver_lakehouse.tablon_leads_and_deals existing
# MAGIC         WHERE existing.cod_Lead = tablon_leads_and_deals.cod_Lead
# MAGIC         AND existing.id_tipo_registro = 2
# MAGIC     )
# MAGIC     OR 
# MAGIC     -- Si existe un estado 2 con la misma `cod_Oportunidad`
# MAGIC     EXISTS (
# MAGIC         SELECT 1 FROM silver_lakehouse.tablon_leads_and_deals existing
# MAGIC         WHERE existing.cod_Oportunidad = tablon_leads_and_deals.cod_Oportunidad
# MAGIC         AND existing.id_tipo_registro = 2
# MAGIC     )
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cod_Lead, COUNT(*)
# MAGIC FROM silver_lakehouse.tablon_leads_and_deals
# MAGIC GROUP BY cod_Lead
# MAGIC HAVING COUNT(*) > 1;
