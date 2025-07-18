# Databricks notebook source
# MAGIC %md
# MAGIC ### Cruce con tablas ZOHO

# COMMAND ----------

# DBTITLE 1,Create view contactos_unidos
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW contactos_unidos AS
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     email,
# MAGIC     phone,
# MAGIC     First_Name,
# MAGIC     Last_Name,
# MAGIC     mailing_city,
# MAGIC     provincia,
# MAGIC     mailing_street,
# MAGIC     mailing_zip,
# MAGIC     nacionalidad,
# MAGIC     sourcesystem
# MAGIC FROM silver_lakehouse.zohocontacts
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     email,
# MAGIC     phone,
# MAGIC     First_Name,
# MAGIC     Last_Name,
# MAGIC     mailing_city,
# MAGIC     other_city AS provincia,
# MAGIC     mailing_street,
# MAGIC     mailing_zip,
# MAGIC     nacionalidad,
# MAGIC     sourcesystem
# MAGIC FROM silver_lakehouse.ZohoContacts_38b;
# MAGIC
# MAGIC --select * from contactos_unidos;

# COMMAND ----------

# DBTITLE 1,Cruce tablon and contactos_unidos
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW zoho_table_view AS
# MAGIC    SELECT 
# MAGIC           tablon.id_tipo_registro
# MAGIC          ,tablon.tipo_registro
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN cod_Lead
# MAGIC              WHEN tablon.id_tipo_registro = 2 THEN cod_Lead
# MAGIC              WHEN tablon.id_tipo_registro = 3 THEN NULL
# MAGIC              ELSE NULL
# MAGIC          END AS cod_Lead
# MAGIC          ,tablon.cod_Oportunidad
# MAGIC          ,COALESCE(Nombre, nombre_Oportunidad) AS nombre
# MAGIC          ,COALESCE(CASE WHEN tablon.id_tipo_registro = 1 THEN tablon.email  -- Email de LEAD
# MAGIC              END, 
# MAGIC              contacts.email  -- Email de CONTACT
# MAGIC          ) AS email
# MAGIC          ,COALESCE(CASE WHEN tablon.id_tipo_registro = 1 THEN tablon.telefono1  -- telefono1 de LEAD
# MAGIC              END, 
# MAGIC              contacts.phone  -- phone de CONTACT
# MAGIC          ) AS telefono
# MAGIC          ,COALESCE(CASE WHEN tablon.id_tipo_registro = 1 THEN CONCAT(tablon.Nombre, ' ', tablon.Apellido1, ' ', tablon.Apellido2)  -- LEAD
# MAGIC              END, 
# MAGIC              CONCAT(contacts.First_Name, ' ', contacts.Last_Name, ' ', tablon.Apellido2)  -- CONTACT
# MAGIC          ) AS nombre_Contacto
# MAGIC          ,CASE WHEN tablon.id_tipo_registro = 1 THEN 0
# MAGIC                WHEN tablon.id_tipo_registro IN (2,3) THEN COALESCE(ABS(tablon.pct_Descuento), 0)
# MAGIC                ELSE NULL
# MAGIC          END AS importe_Descuento
# MAGIC          ,CASE WHEN tablon.id_tipo_registro = 1 THEN 0
# MAGIC                WHEN tablon.id_tipo_registro IN (2,3) THEN COALESCE(ABS(tablon.importe), 0)
# MAGIC                ELSE NULL
# MAGIC          END AS Importe_Venta_Neto
# MAGIC          ,CASE WHEN tablon.id_tipo_registro = 1 THEN 0
# MAGIC              WHEN tablon.id_tipo_registro IN (2,3) THEN COALESCE(ABS(tablon.importe) + ABS(tablon.pct_Descuento), 0)
# MAGIC              ELSE NULL
# MAGIC          END AS Importe_Venta
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN 0
# MAGIC              WHEN tablon.id_tipo_registro IN (2,3) THEN COALESCE(tablon.probabilidad_Conversion / 100, 0)
# MAGIC              ELSE 0
# MAGIC          END AS posibilidad_Venta
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro IN (1,2) THEN tablon.fecha_Creacion_Lead
# MAGIC              WHEN tablon.id_tipo_registro = 3 THEN tablon.fecha_Creacion_Oportunidad
# MAGIC              ELSE NULL
# MAGIC          END AS fec_Creacion
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN tablon.fecha_Modificacion_Lead
# MAGIC              WHEN tablon.id_tipo_registro IN (2,3) THEN tablon.fecha_Modificacion_Oportunidad
# MAGIC              ELSE NULL
# MAGIC          END AS fec_Modificacion
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN NULL
# MAGIC              WHEN tablon.id_tipo_registro IN (2,3) THEN tablon.fecha_Cierre
# MAGIC              ELSE NULL
# MAGIC          END AS fec_Cierre
# MAGIC          ,   CASE 
# MAGIC              WHEN tablon.id_tipo_registro = 1 THEN NULL
# MAGIC              WHEN tablon.id_tipo_registro IN (2,3) THEN tablon.fecha_hora_Pagado
# MAGIC              ELSE NULL
# MAGIC          END AS fec_Pago_Matricula
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro IN (1,2) THEN tablon.lead_Rating
# MAGIC              WHEN tablon.id_tipo_registro = 3 THEN NULL
# MAGIC          END AS nombre_Scoring
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro IN (1,2) THEN tablon.leadScoring
# MAGIC              WHEN tablon.id_tipo_registro = 3 THEN NULL
# MAGIC          END AS puntos_Scoring
# MAGIC          ,CASE 
# MAGIC              WHEN tablon.id_tipo_registro IN (1,2) THEN DATEDIFF(tablon.fecha_Cierre, tablon.fecha_Creacion_Lead)
# MAGIC              WHEN tablon.id_tipo_registro = 3 THEN DATEDIFF(tablon.fecha_Cierre, tablon.fecha_Creacion_Oportunidad)
# MAGIC          END AS dias_Cierre
# MAGIC          ,contacts.mailing_city AS ciudad
# MAGIC          ,contacts.provincia AS provincia
# MAGIC          ,contacts.mailing_street AS calle
# MAGIC          ,contacts.mailing_zip AS codigo_postal
# MAGIC          ,CASE 
# MAGIC               WHEN tablon.id_tipo_registro = 1 THEN tablon.nacionalidad
# MAGIC               WHEN tablon.id_tipo_registro IN (2,3) THEN contacts.nacionalidad
# MAGIC               ELSE NULL
# MAGIC              END nacionalidad
# MAGIC          ,tablon.fecha_hora_anulacion
# MAGIC          ,tablon.fecha_Modificacion_Oportunidad AS fecha_Modificacion_Oportunidad
# MAGIC          ,tablon.fecha_Modificacion_Lead AS fecha_Modificacion_Lead
# MAGIC          ,tablon.id_classlife AS id_classlife
# MAGIC          ,tablon.cod_Owner AS cod_Owner
# MAGIC          ,tablon.nombre_estado_venta AS nombre_estado_venta
# MAGIC          ,tablon.etapa AS etapa
# MAGIC          ,tablon.cod_Producto AS cod_Producto
# MAGIC          ,tablon.tipo_Cliente_lead AS tipo_Cliente_lead
# MAGIC          ,tablon.residencia AS residencia
# MAGIC          ,tablon.motivo_Perdida AS motivo_Perdida
# MAGIC          ,tablon.linea_de_negocio
# MAGIC          ,coalesce(tablon.tipo_Conversion_opotunidad,tablon.tipo_conversion_lead) as tipo_conversion 
# MAGIC          ,tablon.utm_ad_id AS utm_ad_id
# MAGIC          ,tablon.utm_adset_id AS utm_adset_id
# MAGIC          ,tablon.utm_campaign_id AS utm_campaign_id
# MAGIC          ,tablon.utm_campaign_name AS utm_campaign_name
# MAGIC          ,tablon.utm_channel AS utm_channel
# MAGIC          ,tablon.utm_estrategia AS utm_estrategia
# MAGIC          ,tablon.utm_medium AS utm_medium
# MAGIC          ,tablon.utm_perfil AS utm_perfil
# MAGIC          ,tablon.utm_source AS utm_source
# MAGIC          ,tablon.utm_term AS utm_term
# MAGIC          ,tablon.utm_type AS utm_type
# MAGIC          --,contacts.sourcesystem
# MAGIC      FROM silver_lakehouse.tablon_leads_and_deals tablon
# MAGIC LEFT JOIN contactos_unidos contacts
# MAGIC        ON tablon.cod_Contacto = contacts.id;
# MAGIC
# MAGIC --select * from zoho_table_view;

# COMMAND ----------

# DBTITLE 1,Create view zoho_dimensions
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW zoho_dimensions_temp AS
# MAGIC SELECT 
# MAGIC        tablon.id_tipo_registro
# MAGIC       ,tablon.tipo_registro
# MAGIC       ,tablon.cod_Lead
# MAGIC       ,tablon.cod_Oportunidad
# MAGIC       ,tablon.nombre
# MAGIC       ,tablon.email
# MAGIC       ,tablon.telefono
# MAGIC       ,tablon.nombre_contacto
# MAGIC       ,tablon.importe_venta
# MAGIC       ,tablon.importe_descuento
# MAGIC       ,tablon.importe_venta_neto
# MAGIC       ,COALESCE(estadoventa.id_dim_estado_venta, -1) AS id_dim_estado_venta
# MAGIC       ,COALESCE(etapaventa.id_dim_etapa_venta, -1) AS id_dim_etapa_venta
# MAGIC       ,tablon.posibilidad_venta
# MAGIC       ,tablon.fec_creacion
# MAGIC       ,tablon.fec_modificacion
# MAGIC       ,tablon.fec_cierre
# MAGIC       ,tablon.fecha_hora_anulacion
# MAGIC       ,0 AS importe_matricula
# MAGIC       ,0 AS importe_descuento_matricula
# MAGIC       ,0 AS importe_neto_matricula
# MAGIC       ,tablon.ciudad AS ciudad
# MAGIC       ,tablon.provincia AS provincia
# MAGIC       ,tablon.calle AS calle
# MAGIC       ,tablon.codigo_postal AS codigo_postal
# MAGIC       ,tablon.fec_pago_matricula AS fec_pago_matricula
# MAGIC       ,tablon.nombre_scoring AS nombre_scoring
# MAGIC       ,COALESCE(tablon.puntos_scoring, 0) AS puntos_scoring
# MAGIC       ,COALESCE(tablon.dias_Cierre, 0) AS dias_cierre
# MAGIC       ,COALESCE(etapaventa.esNE, 0) AS kpi_new_enrollent
# MAGIC       --,perdida.esNeto AS kpi_lead_neto
# MAGIC       --,perdida.esBruto AS kpi_lead_bruto
# MAGIC       --AIRCALL en METRODORAFP y OCEANO son descartados
# MAGIC       ,CASE WHEN tablon.linea_de_negocio in ('METRODORA FP', 'OCEANO') and upper(tablon.nombre) like 'AIRCALL%' THEN 0 
# MAGIC             ELSE COALESCE(perdida.esNeto, 1) END AS kpi_lead_neto
# MAGIC       ,CASE WHEN tablon.linea_de_negocio in ('METRODORA FP', 'OCEANO') and upper(tablon.nombre) like 'AIRCALL%' THEN 0 
# MAGIC             ELSE COALESCE(perdida.esBruto,1) END AS kpi_lead_bruto
# MAGIC       ,1 AS activo --PENDIENTE DEFINIR LOGICA POR PARTE DEL CLIENTE
# MAGIC       ,tablon.fecha_Modificacion_Oportunidad AS fecha_Modificacion_Oportunidad
# MAGIC       ,tablon.fecha_Modificacion_Lead AS fecha_Modificacion_Lead
# MAGIC       ,tablon.id_classlife AS id_classlife
# MAGIC       ,COALESCE(comercial.id_dim_comercial, -1) AS id_dim_propietario_lead
# MAGIC       ,COALESCE(programa.id_Dim_Programa, -1) AS id_dim_programa
# MAGIC       ,COALESCE(modalidad.id_dim_modalidad, -1) AS id_dim_modalidad
# MAGIC       ,COALESCE(institucion.id_dim_institucion, COALESCE(institucion2.id_dim_institucion, -1)) AS id_dim_institucion
# MAGIC       ,COALESCE(sede.id_dim_sede, -1) AS id_dim_sede
# MAGIC       ,COALESCE(producto.id_Dim_Producto, -1) AS id_dim_producto
# MAGIC       ,COALESCE(formacion.id_dim_tipo_formacion, -1) AS id_dim_tipo_formacion
# MAGIC       ,COALESCE(tiponegocio.id_dim_tipo_negocio, -1) AS id_dim_tipo_negocio
# MAGIC       ,COALESCE(pais.id, -1) AS id_dim_pais
# MAGIC       ,COALESCE(perdida.id_Dim_Motivo_Perdida, -1) AS id_dim_motivo_perdida
# MAGIC       ,COALESCE(nacionalidad.id, -1) AS id_dim_nacionalidad
# MAGIC       ,COALESCE(vertical.id_Dim_Vertical, -1) AS id_dim_vertical
# MAGIC       ,COALESCE(conversion.id_Dim_Tipo_Conversion, -1) AS id_dim_tipo_conversion
# MAGIC       ,COALESCE(dim_utm_ad.id_dim_utm_ad, -1) AS id_dim_utm_ad
# MAGIC       ,COALESCE(utmadset.id_utm_adset_id, -1) AS id_dim_utm_adset
# MAGIC       ,COALESCE(utmcampaign.id_utm_campaign_id, -1) AS id_dim_utm_campaign
# MAGIC       ,COALESCE(utmcampaignname.id_utm_campaign_name, -1) AS id_dim_utm_campaign_name
# MAGIC       ,COALESCE(utmchannel.id_utm_channel, -1) AS id_dim_utm_channel
# MAGIC       ,COALESCE(utmestrategia.id_utm_estrategia, -1) AS id_dim_utm_estrategia
# MAGIC       ,COALESCE(utmmedium.id_utm_medium, -1) AS id_dim_utm_medium
# MAGIC       ,COALESCE(utmperfil.id_utm_perfil, -1) AS id_dim_utm_perfil
# MAGIC       ,COALESCE(utmsource.id_utm_source, -1) AS id_dim_utm_source
# MAGIC       ,COALESCE(utmterm.id_utm_term, -1) AS id_dim_utm_term
# MAGIC       ,COALESCE(utmtype.id_utm_type, -1) AS id_dim_utm_type
# MAGIC       ,current_timestamp AS ETLcreatedDate
# MAGIC       ,current_timestamp AS ETLupdatedDate
# MAGIC FROM zoho_table_view tablon 
# MAGIC LEFT JOIN gold_lakehouse.dim_comercial comercial ON tablon.cod_Owner = comercial.cod_comercial AND comercial.activo = 1
# MAGIC LEFT JOIN gold_lakehouse.dim_estado_venta estadoventa ON tablon.nombre_estado_venta = estadoventa.nombre_estado_venta
# MAGIC LEFT JOIN gold_lakehouse.dim_etapa_venta etapaventa ON tablon.etapa = etapaventa.nombre_etapa_venta
# MAGIC LEFT JOIN gold_lakehouse.dim_programa programa ON SUBSTRING(tablon.cod_Producto, 10, 5) = UPPER(programa.cod_Programa)
# MAGIC LEFT JOIN gold_lakehouse.dim_modalidad modalidad ON SUBSTRING(tablon.cod_Producto, 18, 1) = SUBSTRING(modalidad.nombre_modalidad,1,1)
# MAGIC LEFT JOIN gold_lakehouse.dim_producto producto ON tablon.cod_Producto = NULLIF(producto.cod_Producto, '')
# MAGIC --LEFT JOIN gold_lakehouse.dim_institucion institucion ON UPPER(programa.entidad_Legal) = NULLIF(UPPER(institucion.nombre_institucion), '') 
# MAGIC -- Primero que cruce por el producto, si tiene
# MAGIC LEFT JOIN (select ent.entidad_legal, ins.id_dim_institucion from gold_lakehouse.dim_institucion ins
# MAGIC             left join silver_lakehouse.entidad_legal ent on ent.institucion = ins.nombre_institucion) institucion ON UPPER(producto.entidad_Legal) = NULLIF(UPPER(institucion.entidad_legal), '')
# MAGIC --Si no tiene producto, por la linea de negocio del objeto de Zoho (FISIOFOCUS NO ESTÁ COMO ENTIDAD LEGAL, ES METRODORA LEARNING)
# MAGIC LEFT JOIN gold_lakehouse.dim_institucion institucion2 ON 
# MAGIC CASE WHEN UPPER(tablon.linea_de_negocio) = 'FISIOFOCUS' then 'METRODORA LEARNING' else UPPER(tablon.linea_de_negocio) end = NULLIF(UPPER(institucion2.nombre_institucion), '') 
# MAGIC LEFT JOIN gold_lakehouse.dim_sede sede ON SUBSTRING(tablon.cod_Producto, 20, 3) = NULLIF(sede.codigo_sede, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_tipo_formacion formacion ON programa.tipo_Programa = NULLIF(formacion.tipo_formacion_desc, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_tipo_negocio tiponegocio ON tablon.tipo_Cliente_lead = NULLIF(tiponegocio.tipo_negocio_desc, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_pais pais ON UPPER(tablon.residencia) = NULLIF(UPPER(pais.nombre), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_nacionalidad nacionalidad ON UPPER(pais.nombre) = NULLIF(UPPER(nacionalidad.nombre), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_motivo_perdida perdida ON UPPER(tablon.motivo_Perdida) = NULLIF(UPPER(perdida.nombre_Dim_Motivo_Perdida), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_vertical vertical ON UPPER(producto.cod_Vertical) = NULLIF(UPPER(vertical.nombre_Vertical_Corto), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_tipo_conversion conversion ON UPPER(tablon.tipo_conversion) = NULLIF(UPPER(conversion.tipo_conversion), '')
# MAGIC ---Cruze dim_utms
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_ad dim_utm_ad ON UPPER(tablon.utm_ad_id) = NULLIF(UPPER(dim_utm_ad.utm_ad_id), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_adset utmadset ON UPPER(tablon.utm_adset_id) = NULLIF(UPPER(utmadset.utm_adset_id), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_campaign utmcampaign ON UPPER(tablon.utm_campaign_id) = NULLIF(UPPER(utmcampaign.utm_campaign_id), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_campaign_name utmcampaignname ON UPPER(tablon.utm_campaign_name) = NULLIF(UPPER(utmcampaignname.utm_campaign_name), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_channel utmchannel ON UPPER(tablon.utm_channel) = NULLIF(UPPER(utmchannel.utm_channel), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_estrategia utmestrategia ON UPPER(tablon.utm_estrategia) = NULLIF(UPPER(utmestrategia.utm_estrategia), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_medium utmmedium ON UPPER(tablon.utm_medium) = NULLIF(UPPER(utmmedium.utm_medium), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_perfil utmperfil ON UPPER(tablon.utm_perfil) = NULLIF(UPPER(utmperfil.utm_perfil), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_source utmsource ON UPPER(tablon.utm_source) = NULLIF(UPPER(utmsource.utm_source), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_term utmterm ON UPPER(tablon.utm_term) = NULLIF(UPPER(utmterm.utm_term), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_type utmtype ON UPPER(tablon.utm_type) = NULLIF(UPPER(utmtype.utm_type), '');
# MAGIC
# MAGIC --select * from zoho_dimensions_temp;

# COMMAND ----------

# DBTITLE 1,Count duplicates cod_Lead
# MAGIC %sql
# MAGIC SELECT cod_Lead, COUNT(*) 
# MAGIC FROM zoho_dimensions_temp 
# MAGIC GROUP BY cod_Lead 
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC --SELECT cod_Oportunidad, COUNT(*) 
# MAGIC --FROM zoho_dimensions_temp 
# MAGIC --GROUP BY cod_Oportunidad 
# MAGIC --HAVING COUNT(*) > 1;

# COMMAND ----------

# DBTITLE 1,Deleted Duplicates
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fct_zoho_unique_temp AS
# MAGIC SELECT * FROM (
# MAGIC     SELECT *, 
# MAGIC            ROW_NUMBER() OVER (
# MAGIC                PARTITION BY COALESCE(cod_Lead, ''), COALESCE(cod_Oportunidad, '') ORDER BY id_dim_propietario_lead DESC
# MAGIC            ) AS rn
# MAGIC     FROM zoho_dimensions_temp
# MAGIC ) filtered
# MAGIC WHERE rn = 1;  -- 🔹 Solo conserva la versión más reciente
# MAGIC
# MAGIC --select * from fct_zoho_unique_temp;

# COMMAND ----------

# MAGIC %sql select source.cod_lead, source.cod_oportunidad, source.fecha_Modificacion_Lead, target.fecha_modificacion_lead, source.fecha_modificacion_oportunidad, target.fecha_modificacion_oportunidad from fct_zoho_unique_temp as source 
# MAGIC inner join gold_lakehouse.fctventa AS target ON COALESCE(target.cod_Oportunidad, '') = COALESCE(source.cod_Oportunidad, '')
# MAGIC           AND COALESCE(target.cod_Lead, '') = COALESCE(source.cod_Lead, '')
# MAGIC where source.cod_oportunidad= 820629000008282160;

# COMMAND ----------

# DBTITLE 1,Estado 1 → Estado 2
# MAGIC %sql
# MAGIC -- 1️⃣ Actualizar registros tipo 1 que evolucionan a tipo 2
# MAGIC MERGE INTO gold_lakehouse.fctventa AS target
# MAGIC USING fct_zoho_unique_temp AS source
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
# MAGIC         target.importe_venta = source.importe_venta,
# MAGIC         target.importe_descuento = source.importe_descuento,
# MAGIC         target.importe_venta_neto = source.importe_venta_neto,
# MAGIC         target.id_dim_estado_venta = source.id_dim_estado_venta,
# MAGIC         target.id_dim_etapa_venta = source.id_dim_etapa_venta,
# MAGIC         target.posibilidad_venta = source.posibilidad_venta,
# MAGIC         target.fec_cierre = source.fec_cierre,
# MAGIC         target.fecha_hora_anulacion = source.fecha_hora_anulacion,
# MAGIC         target.importe_matricula = source.importe_matricula,
# MAGIC         target.importe_descuento_matricula = source.importe_descuento_matricula,
# MAGIC         target.importe_neto_matricula = source.importe_neto_matricula,
# MAGIC         target.ciudad = source.ciudad,
# MAGIC         target.provincia = source.provincia,
# MAGIC         target.calle = source.calle,
# MAGIC         target.codigo_postal = source.codigo_postal,
# MAGIC         target.fec_pago_matricula = source.fec_pago_matricula,
# MAGIC         target.dias_cierre = source.dias_cierre,
# MAGIC         target.kpi_new_enrollent = source.kpi_new_enrollent,
# MAGIC         target.kpi_lead_neto = source.kpi_lead_neto,
# MAGIC         target.kpi_lead_bruto = source.kpi_lead_bruto,
# MAGIC         target.id_classlife = source.id_classlife,
# MAGIC         target.id_dim_propietario_lead = source.id_dim_propietario_lead,
# MAGIC         target.id_dim_programa = source.id_dim_programa,
# MAGIC         target.id_dim_modalidad = source.id_dim_modalidad,
# MAGIC         target.id_dim_institucion = source.id_dim_institucion,
# MAGIC         target.id_dim_sede = source.id_dim_sede,
# MAGIC         target.id_dim_Producto = source.id_dim_Producto,
# MAGIC         target.id_dim_tipo_formacion = source.id_dim_tipo_formacion,
# MAGIC         target.id_dim_tipo_negocio = source.id_dim_tipo_negocio,
# MAGIC         target.id_dim_pais = source.id_dim_pais,
# MAGIC         target.id_dim_motivo_perdida = source.id_dim_motivo_perdida,
# MAGIC         target.id_dim_nacionalidad = source.id_dim_nacionalidad,
# MAGIC         target.activo = source.activo,
# MAGIC         target.id_dim_vertical = source.id_dim_vertical,
# MAGIC         target.id_dim_tipo_conversion = source.id_dim_tipo_conversion,
# MAGIC         target.id_dim_utm_ad = source.id_dim_utm_ad,
# MAGIC         target.id_dim_utm_adset = source.id_dim_utm_adset,
# MAGIC         target.id_dim_utm_campaign = source.id_dim_utm_campaign,
# MAGIC         target.id_dim_utm_campaign_name = source.id_dim_utm_campaign_name,
# MAGIC         target.id_dim_utm_channel = source.id_dim_utm_channel,
# MAGIC         target.id_dim_utm_estrategia = source.id_dim_utm_estrategia,
# MAGIC         target.id_dim_utm_medium = source.id_dim_utm_medium,
# MAGIC         target.id_dim_utm_perfil = source.id_dim_utm_perfil,
# MAGIC         target.id_dim_utm_source = source.id_dim_utm_source,
# MAGIC         target.id_dim_utm_term = source.id_dim_utm_term,
# MAGIC         target.id_dim_utm_type = source.id_dim_utm_type,
# MAGIC         target.fec_Modificacion = source.fec_Modificacion,
# MAGIC         target.ETLupdatedDate = current_timestamp;

# COMMAND ----------

# DBTITLE 1,Estado 3 → Estado 2
# MAGIC %sql
# MAGIC -- 2️⃣ Actualizar registros tipo 3 que evolucionan a tipo 2
# MAGIC MERGE INTO gold_lakehouse.fctventa AS target
# MAGIC USING fct_zoho_unique_temp AS source
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
# MAGIC         target.importe_venta = source.importe_venta,
# MAGIC         target.importe_descuento = source.importe_descuento,
# MAGIC         target.importe_venta_neto = source.importe_venta_neto,
# MAGIC         target.id_dim_estado_venta = source.id_dim_estado_venta,
# MAGIC         target.id_dim_etapa_venta = source.id_dim_etapa_venta,
# MAGIC         target.posibilidad_venta = source.posibilidad_venta,
# MAGIC         target.fec_cierre = source.fec_cierre,
# MAGIC         target.fecha_hora_anulacion = source.fecha_hora_anulacion,
# MAGIC         target.importe_matricula = source.importe_matricula,
# MAGIC         target.importe_descuento_matricula = source.importe_descuento_matricula,
# MAGIC         target.importe_neto_matricula = source.importe_neto_matricula,
# MAGIC         target.ciudad = source.ciudad,
# MAGIC         target.provincia = source.provincia,
# MAGIC         target.calle = source.calle,
# MAGIC         target.codigo_postal = source.codigo_postal,
# MAGIC         target.fec_pago_matricula = source.fec_pago_matricula,
# MAGIC         target.dias_cierre = source.dias_cierre,
# MAGIC         target.kpi_new_enrollent = source.kpi_new_enrollent,
# MAGIC         target.kpi_lead_neto = source.kpi_lead_neto,
# MAGIC         target.kpi_lead_bruto = source.kpi_lead_bruto,
# MAGIC         target.id_classlife = source.id_classlife,
# MAGIC         target.id_dim_propietario_lead = source.id_dim_propietario_lead,
# MAGIC         target.id_dim_programa = source.id_dim_programa,
# MAGIC         target.id_dim_modalidad = source.id_dim_modalidad,
# MAGIC         target.id_dim_institucion = source.id_dim_institucion,
# MAGIC         target.id_dim_sede = source.id_dim_sede,
# MAGIC         target.id_dim_Producto = source.id_dim_Producto,
# MAGIC         target.id_dim_tipo_formacion = source.id_dim_tipo_formacion,
# MAGIC         target.id_dim_tipo_negocio = source.id_dim_tipo_negocio,
# MAGIC         target.id_dim_pais = source.id_dim_pais,
# MAGIC         target.id_dim_motivo_perdida = source.id_dim_motivo_perdida,
# MAGIC         target.id_dim_nacionalidad = source.id_dim_nacionalidad,
# MAGIC         target.activo = source.activo,
# MAGIC         target.id_dim_vertical = source.id_dim_vertical,
# MAGIC         target.id_dim_tipo_conversion = source.id_dim_tipo_conversion,
# MAGIC         target.id_dim_utm_ad = source.id_dim_utm_ad,
# MAGIC         target.id_dim_utm_adset = source.id_dim_utm_adset,
# MAGIC         target.id_dim_utm_campaign = source.id_dim_utm_campaign,
# MAGIC         target.id_dim_utm_campaign_name = source.id_dim_utm_campaign_name,
# MAGIC         target.id_dim_utm_channel = source.id_dim_utm_channel,
# MAGIC         target.id_dim_utm_estrategia = source.id_dim_utm_estrategia,
# MAGIC         target.id_dim_utm_medium = source.id_dim_utm_medium,
# MAGIC         target.id_dim_utm_perfil = source.id_dim_utm_perfil,
# MAGIC         target.id_dim_utm_source = source.id_dim_utm_source,
# MAGIC         target.id_dim_utm_term = source.id_dim_utm_term,
# MAGIC         target.id_dim_utm_type = source.id_dim_utm_type,
# MAGIC         target.fec_Modificacion = source.fec_Modificacion,
# MAGIC         target.ETLupdatedDate = current_timestamp;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Insertar Nuevos Registros si No Existen
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.fctventa AS target
# MAGIC USING fct_zoho_unique_temp AS source
# MAGIC ON COALESCE(target.cod_Lead, '') = COALESCE(source.cod_Lead, '') 
# MAGIC AND COALESCE(target.cod_Oportunidad, '') = COALESCE(source.cod_Oportunidad, '')
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         cod_lead, cod_oportunidad, nombre, email, telefono, nombre_contacto, 
# MAGIC         importe_venta, importe_descuento, importe_venta_neto, posibilidad_venta, 
# MAGIC         ciudad, provincia, calle, codigo_postal, nombre_scoring, puntos_scoring, dias_cierre, 
# MAGIC         fec_creacion, fec_modificacion, fec_cierre, fec_pago_matricula, fecha_hora_anulacion, 
# MAGIC         fecha_Modificacion_Lead, fecha_Modificacion_Oportunidad, importe_matricula, 
# MAGIC         importe_descuento_matricula, importe_neto_matricula, kpi_new_enrollent, kpi_lead_neto, 
# MAGIC         kpi_lead_bruto, activo, id_classlife, id_tipo_registro, tipo_registro, id_dim_propietario_lead, 
# MAGIC         id_dim_programa, id_dim_producto, id_dim_nacionalidad, id_dim_tipo_formacion, id_dim_tipo_negocio, id_dim_modalidad, 
# MAGIC         id_dim_institucion, id_dim_sede, id_dim_pais, id_dim_estado_venta, id_dim_etapa_venta, 
# MAGIC         id_dim_motivo_perdida, id_dim_vertical, id_dim_tipo_conversion,
# MAGIC         id_dim_utm_ad, id_dim_utm_adset, id_dim_utm_campaign, id_dim_utm_campaign_name, 
# MAGIC         id_dim_utm_channel, id_dim_utm_estrategia, id_dim_utm_medium, id_dim_utm_perfil, 
# MAGIC         id_dim_utm_source, id_dim_utm_term, id_dim_utm_type, ETLcreatedDate, ETLupdatedDate
# MAGIC     ) 
# MAGIC     VALUES (
# MAGIC         source.cod_lead, source.cod_oportunidad, source.nombre, source.email, source.telefono, 
# MAGIC         source.nombre_contacto, source.importe_venta, source.importe_descuento, source.importe_venta_neto, 
# MAGIC         source.posibilidad_venta, source.ciudad, source.provincia, source.calle, source.codigo_postal, 
# MAGIC         source.nombre_scoring, source.puntos_scoring, source.dias_cierre, source.fec_creacion, 
# MAGIC         source.fec_modificacion, source.fec_cierre, source.fec_pago_matricula, source.fecha_hora_anulacion, 
# MAGIC         source.fecha_Modificacion_Lead, source.fecha_Modificacion_Oportunidad, source.importe_matricula, 
# MAGIC         source.importe_descuento_matricula, source.importe_neto_matricula, source.kpi_new_enrollent, 
# MAGIC         source.kpi_lead_neto, source.kpi_lead_bruto, source.activo, source.id_classlife, source.id_tipo_registro, 
# MAGIC         source.tipo_registro, source.id_dim_propietario_lead, source.id_dim_programa, 
# MAGIC         source.id_dim_producto, source.id_dim_nacionalidad, source.id_dim_tipo_formacion, 
# MAGIC         source.id_dim_tipo_negocio, source.id_dim_modalidad, source.id_dim_institucion, 
# MAGIC         source.id_dim_sede, source.id_dim_pais, source.id_dim_estado_venta, source.id_dim_etapa_venta, 
# MAGIC         source.id_dim_motivo_perdida, source.id_dim_vertical, source.id_dim_tipo_conversion, 
# MAGIC         source.id_dim_utm_ad,source.id_dim_utm_adset,source.id_dim_utm_campaign,source.id_dim_utm_campaign_name,source.id_dim_utm_channel,source.id_dim_utm_estrategia,
# MAGIC         source.id_dim_utm_medium,source.id_dim_utm_perfil,source.id_dim_utm_source,source.id_dim_utm_term,source.id_dim_utm_type, 
# MAGIC         current_timestamp, current_timestamp
# MAGIC     );

# COMMAND ----------

# DBTITLE 1,Registros por fecha_Modificacion_Lead u Oportunidad que no han cambiado de estado y actualizan campos
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.fctventa AS target
# MAGIC USING fct_zoho_unique_temp AS source
# MAGIC ON COALESCE(target.cod_Oportunidad, '') = COALESCE(source.cod_Oportunidad, '')
# MAGIC    AND COALESCE(target.cod_Lead, '') = COALESCE(source.cod_Lead, '')
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC    -- source.fecha_Modificacion_Lead IS DISTINCT FROM target.fecha_Modificacion_Lead OR
# MAGIC     --source.kpi_lead_neto IS DISTINCT FROM target.kpi_lead_neto OR
# MAGIC    -- source.kpi_lead_bruto IS DISTINCT FROM target.kpi_lead_bruto 
# MAGIC    source.fecha_Modificacion_Lead > target.fecha_Modificacion_Lead OR
# MAGIC    source.fecha_Modificacion_Oportunidad > target.fecha_Modificacion_Oportunidad OR
# MAGIC    source.kpi_lead_neto <> target.kpi_lead_neto OR
# MAGIC    source.kpi_lead_bruto <> target.kpi_lead_bruto OR
# MAGIC    source.fec_pago_matricula <> target.fec_pago_matricula
# MAGIC )
# MAGIC  THEN
# MAGIC UPDATE SET
# MAGIC     target.importe_venta = source.importe_venta,
# MAGIC     target.importe_descuento = source.importe_descuento,
# MAGIC     target.importe_venta_neto = source.importe_venta_neto,
# MAGIC     target.posibilidad_venta = source.posibilidad_venta,
# MAGIC     target.ciudad = source.ciudad,
# MAGIC     target.provincia = source.provincia,
# MAGIC     target.dias_cierre = source.dias_cierre,
# MAGIC     target.fec_creacion = source.fec_creacion,
# MAGIC     target.fec_modificacion = source.fec_modificacion,
# MAGIC     target.fec_cierre = source.fec_cierre,
# MAGIC     target.fec_pago_matricula = source.fec_pago_matricula,
# MAGIC     target.fecha_hora_anulacion = source.fecha_hora_anulacion,
# MAGIC     target.fecha_Modificacion_Lead = source.fecha_Modificacion_Lead,
# MAGIC     target.importe_matricula = source.importe_matricula,
# MAGIC     target.importe_descuento_matricula = source.importe_descuento_matricula,
# MAGIC     target.importe_neto_matricula = source.importe_neto_matricula,
# MAGIC     target.kpi_new_enrollent = source.kpi_new_enrollent,
# MAGIC     target.kpi_lead_neto = source.kpi_lead_neto,
# MAGIC     target.kpi_lead_bruto = source.kpi_lead_bruto,
# MAGIC     target.activo = source.activo,
# MAGIC     target.id_classlife = source.id_classlife,
# MAGIC     target.id_tipo_registro = source.id_tipo_registro,
# MAGIC     target.tipo_registro = source.tipo_registro,
# MAGIC     target.id_dim_propietario_lead = source.id_dim_propietario_lead,
# MAGIC     target.id_dim_programa = source.id_dim_programa,
# MAGIC     target.id_dim_producto = source.id_dim_producto,
# MAGIC     target.id_dim_nacionalidad = source.id_dim_nacionalidad,
# MAGIC     target.id_dim_tipo_formacion = source.id_dim_tipo_formacion,
# MAGIC     target.id_dim_tipo_negocio = source.id_dim_tipo_negocio,
# MAGIC     target.id_dim_modalidad = source.id_dim_modalidad,
# MAGIC     target.id_dim_institucion = source.id_dim_institucion,
# MAGIC     target.id_dim_sede = source.id_dim_sede,
# MAGIC     target.id_dim_pais = source.id_dim_pais,
# MAGIC     target.id_dim_estado_venta = source.id_dim_estado_venta,
# MAGIC     target.id_dim_etapa_venta = source.id_dim_etapa_venta,
# MAGIC     target.id_dim_motivo_perdida = source.id_dim_motivo_perdida,
# MAGIC     target.id_dim_vertical = source.id_dim_vertical,
# MAGIC     target.id_dim_tipo_conversion = source.id_dim_tipo_conversion,
# MAGIC     target.id_dim_utm_ad = source.id_dim_utm_ad,
# MAGIC     target.id_dim_utm_adset = source.id_dim_utm_adset,
# MAGIC     target.id_dim_utm_campaign = source.id_dim_utm_campaign,
# MAGIC     target.id_dim_utm_campaign_name = source.id_dim_utm_campaign_name,
# MAGIC     target.id_dim_utm_channel = source.id_dim_utm_channel,
# MAGIC     target.id_dim_utm_estrategia = source.id_dim_utm_estrategia,
# MAGIC     target.id_dim_utm_medium = source.id_dim_utm_medium,
# MAGIC     target.id_dim_utm_perfil = source.id_dim_utm_perfil,
# MAGIC     target.id_dim_utm_source = source.id_dim_utm_source,
# MAGIC     target.id_dim_utm_term = source.id_dim_utm_term,
# MAGIC     target.id_dim_utm_type = source.id_dim_utm_type,
# MAGIC     target.ETLupdatedDate = source.ETLupdatedDate;    

# COMMAND ----------

# DBTITLE 1,Eliminar Registros Duplicados de Estados Anteriores
# MAGIC %sql
# MAGIC DELETE FROM gold_lakehouse.fctventa AS target
# MAGIC WHERE id_tipo_registro IN (1,3)
# MAGIC AND (
# MAGIC     EXISTS (
# MAGIC         SELECT 1 FROM gold_lakehouse.fctventa AS existing
# MAGIC         WHERE existing.cod_lead = target.cod_lead
# MAGIC         AND existing.id_tipo_registro = 2
# MAGIC     )
# MAGIC     OR 
# MAGIC     EXISTS (
# MAGIC         SELECT 1 FROM gold_lakehouse.fctventa AS existing
# MAGIC         WHERE existing.cod_oportunidad = target.cod_oportunidad
# MAGIC         AND existing.id_tipo_registro = 2
# MAGIC     )
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Count cod_lead duplicates
# MAGIC %sql
# MAGIC SELECT cod_lead, COUNT(*)
# MAGIC FROM gold_lakehouse.fctventa
# MAGIC WHERE cod_lead IS NOT NULL
# MAGIC GROUP BY cod_lead
# MAGIC HAVING COUNT(*) > 1;
