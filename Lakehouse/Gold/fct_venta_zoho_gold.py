# Databricks notebook source
# MAGIC %md
# MAGIC ### Cruce con tablas ZOHO

# COMMAND ----------

# DBTITLE 1,Cruce Zoho
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
# MAGIC          ,tablon.utm_campaign_id AS utm_campaign_id
# MAGIC          ,tablon.utm_ad_id AS utm_ad_id
# MAGIC          ,tablon.utm_source AS utm_source
# MAGIC          ,tablon.linea_de_negocio
# MAGIC      FROM silver_lakehouse.tablon_leads_and_deals tablon
# MAGIC LEFT JOIN silver_lakehouse.zohocontacts contacts
# MAGIC        ON tablon.cod_Contacto = contacts.id;

# COMMAND ----------

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
# MAGIC       ,perdida.esNeto AS kpi_lead_neto
# MAGIC       ,perdida.esBruto AS kpi_lead_bruto
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
# MAGIC       ,COALESCE(utmcampaign.id_dim_utm_campaign, -1) AS id_dim_utm_campaign
# MAGIC       ,COALESCE(utmadset.id_dim_utm_ad, -1) AS id_dim_utm_ad
# MAGIC       ,COALESCE(utmsource.id_dim_utm_source, -1) AS id_dim_utm_source
# MAGIC       ,current_timestamp AS ETLcreatedDate
# MAGIC       ,current_timestamp AS ETLupdatedDate
# MAGIC FROM zoho_table_view tablon 
# MAGIC LEFT JOIN gold_lakehouse.dim_comercial comercial ON tablon.cod_Owner = comercial.cod_comercial 
# MAGIC LEFT JOIN gold_lakehouse.dim_estado_venta estadoventa ON tablon.nombre_estado_venta = estadoventa.nombre_estado_venta
# MAGIC LEFT JOIN gold_lakehouse.dim_etapa_venta etapaventa ON tablon.etapa = etapaventa.nombre_etapa_venta
# MAGIC LEFT JOIN gold_lakehouse.dim_programa programa ON SUBSTRING(tablon.cod_Producto, 10, 5) = UPPER(programa.cod_Programa)
# MAGIC LEFT JOIN gold_lakehouse.dim_modalidad modalidad ON SUBSTRING(tablon.cod_Producto, 18, 1) = SUBSTRING(modalidad.nombre_modalidad,1,1)
# MAGIC LEFT JOIN gold_lakehouse.dim_institucion institucion ON UPPER(programa.entidad_Legal) = NULLIF(UPPER(institucion.nombre_institucion), '') 
# MAGIC LEFT JOIN gold_lakehouse.dim_institucion institucion2 ON UPPER(tablon.linea_de_negocio) = NULLIF(UPPER(institucion2.nombre_institucion), '') 
# MAGIC LEFT JOIN gold_lakehouse.dim_sede sede ON SUBSTRING(tablon.cod_Producto, 20, 3) = NULLIF(sede.codigo_sede, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_producto producto ON tablon.cod_Producto = NULLIF(producto.cod_Producto, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_tipo_formacion formacion ON programa.tipo_Programa = NULLIF(formacion.tipo_formacion_desc, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_tipo_negocio tiponegocio ON tablon.tipo_Cliente_lead = NULLIF(tiponegocio.tipo_negocio_desc, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_pais pais ON UPPER(tablon.residencia) = NULLIF(UPPER(pais.nombre), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_nacionalidad nacionalidad ON UPPER(pais.nombre) = NULLIF(UPPER(nacionalidad.nombre), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_motivo_perdida perdida ON UPPER(tablon.motivo_Perdida) = NULLIF(UPPER(perdida.nombre_Dim_Motivo_Perdida), '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_campaign utmcampaign ON tablon.utm_campaign_id = NULLIF(utmcampaign.utm_campaign_id, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_adset utmadset ON tablon.utm_ad_id = NULLIF(utmadset.utm_ad_id, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_utm_source utmsource ON tablon.utm_source = NULLIF(utmsource.utm_source, '')
# MAGIC LEFT JOIN gold_lakehouse.dim_vertical vertical ON UPPER(producto.cod_Vertical) = NULLIF(UPPER(vertical.nombre_Vertical_Corto), '');
# MAGIC
# MAGIC --select * from zoho_dimensions_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fct_zoho_unique_temp AS
# MAGIC SELECT * FROM (
# MAGIC     SELECT *, 
# MAGIC            ROW_NUMBER() OVER (
# MAGIC                PARTITION BY COALESCE(cod_Lead, ''), COALESCE(cod_Oportunidad, '') 
# MAGIC                ORDER BY id_dim_propietario_lead DESC
# MAGIC            ) AS rn
# MAGIC     FROM zoho_dimensions_temp
# MAGIC ) filtered
# MAGIC WHERE rn = 1;  -- 🔹 Solo conserva la versión más reciente

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
# MAGIC         target.id_dim_utm_campaign = source.id_dim_utm_campaign,
# MAGIC         target.id_dim_utm_ad = source.id_dim_utm_ad,
# MAGIC         target.id_dim_utm_source = source.id_dim_utm_source,
# MAGIC         target.activo = source.activo,
# MAGIC         target.id_dim_vertical = source.id_dim_vertical,
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
# MAGIC         target.id_dim_utm_campaign = source.id_dim_utm_campaign,
# MAGIC         target.id_dim_utm_ad = source.id_dim_utm_ad,
# MAGIC         target.id_dim_utm_source = source.id_dim_utm_source,
# MAGIC         target.activo = source.activo,
# MAGIC         target.id_dim_vertical = source.id_dim_vertical,
# MAGIC         target.fec_Modificacion = source.fec_Modificacion,
# MAGIC         target.ETLupdatedDate = current_timestamp;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Insertar Nuevos Registros si No Existen
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.fctventa AS target
# MAGIC USING fct_zoho_unique_temp AS source
# MAGIC ON 
# MAGIC     COALESCE(target.cod_Lead, '') = COALESCE(source.cod_Lead, '') 
# MAGIC     AND COALESCE(target.cod_Oportunidad, '') = COALESCE(source.cod_Oportunidad, '')
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        target.nombre <> source.nombre
# MAGIC     OR target.email <> source.email
# MAGIC     OR target.telefono <> source.telefono
# MAGIC     OR target.nombre_contacto <> source.nombre_contacto
# MAGIC     OR target.importe_venta <> source.importe_venta
# MAGIC     OR target.importe_descuento <> source.importe_descuento
# MAGIC     OR target.importe_venta_neto <> source.importe_venta_neto
# MAGIC     OR target.posibilidad_venta <> source.posibilidad_venta
# MAGIC     OR target.ciudad <> source.ciudad
# MAGIC     OR target.provincia <> source.provincia
# MAGIC     OR target.calle <> source.calle
# MAGIC     OR target.codigo_postal <> source.codigo_postal
# MAGIC     OR target.nombre_scoring <> source.nombre_scoring
# MAGIC     OR target.puntos_scoring <> source.puntos_scoring
# MAGIC     OR target.dias_cierre <> source.dias_cierre
# MAGIC     OR target.fec_creacion <> source.fec_creacion
# MAGIC     OR target.fec_modificacion <> source.fec_modificacion
# MAGIC     OR target.fec_cierre <> source.fec_cierre
# MAGIC     OR target.fec_pago_matricula <> source.fec_pago_matricula
# MAGIC     OR target.fecha_hora_anulacion <> source.fecha_hora_anulacion
# MAGIC     OR target.fecha_Modificacion_Lead <> source.fecha_Modificacion_Lead
# MAGIC     OR target.fecha_Modificacion_Oportunidad <> source.fecha_Modificacion_Oportunidad
# MAGIC     OR target.importe_matricula <> source.importe_matricula
# MAGIC     OR target.importe_descuento_matricula <> source.importe_descuento_matricula
# MAGIC     OR target.importe_neto_matricula <> source.importe_neto_matricula
# MAGIC     OR target.kpi_new_enrollent <> source.kpi_new_enrollent
# MAGIC     OR target.kpi_lead_neto <> source.kpi_lead_neto
# MAGIC     OR target.kpi_lead_bruto <> source.kpi_lead_bruto
# MAGIC     OR target.activo <> source.activo
# MAGIC     OR target.id_classlife <> source.id_classlife
# MAGIC     OR target.id_tipo_registro <> source.id_tipo_registro
# MAGIC     OR target.tipo_registro <> source.tipo_registro
# MAGIC     OR target.id_dim_propietario_lead <> source.id_dim_propietario_lead
# MAGIC     OR target.id_dim_programa <> source.id_dim_programa
# MAGIC     OR target.id_dim_producto <> source.id_dim_producto
# MAGIC     OR target.id_dim_utm_campaign <> source.id_dim_utm_campaign
# MAGIC     OR target.id_dim_utm_ad <> source.id_dim_utm_ad
# MAGIC     OR target.id_dim_utm_source <> source.id_dim_utm_source
# MAGIC     OR target.id_dim_nacionalidad <> source.id_dim_nacionalidad
# MAGIC     OR target.id_dim_tipo_formacion <> source.id_dim_tipo_formacion
# MAGIC     OR target.id_dim_tipo_negocio <> source.id_dim_tipo_negocio
# MAGIC     OR target.id_dim_modalidad <> source.id_dim_modalidad
# MAGIC     OR target.id_dim_institucion <> source.id_dim_institucion
# MAGIC     OR target.id_dim_sede <> source.id_dim_sede
# MAGIC     OR target.id_dim_pais <> source.id_dim_pais
# MAGIC     OR target.id_dim_estado_venta <> source.id_dim_estado_venta
# MAGIC     OR target.id_dim_etapa_venta <> source.id_dim_etapa_venta
# MAGIC     OR target.id_dim_motivo_perdida <> source.id_dim_motivo_perdida
# MAGIC     OR target.id_dim_vertical <> source.id_dim_vertical
# MAGIC ) 
# MAGIC THEN 
# MAGIC     UPDATE SET
# MAGIC         target.nombre = source.nombre,
# MAGIC         target.email = source.email,
# MAGIC         target.telefono = source.telefono,
# MAGIC         target.nombre_contacto = source.nombre_contacto,
# MAGIC         target.importe_venta = source.importe_venta,
# MAGIC         target.importe_descuento = source.importe_descuento,
# MAGIC         target.importe_venta_neto = source.importe_venta_neto,
# MAGIC         target.posibilidad_venta = source.posibilidad_venta,
# MAGIC         target.ciudad = source.ciudad,
# MAGIC         target.provincia = source.provincia,
# MAGIC         target.calle = source.calle,
# MAGIC         target.codigo_postal = source.codigo_postal,
# MAGIC         target.nombre_scoring = source.nombre_scoring,
# MAGIC         target.puntos_scoring = source.puntos_scoring,
# MAGIC         target.dias_cierre = source.dias_cierre,
# MAGIC         target.fec_creacion = source.fec_creacion,
# MAGIC         target.fec_modificacion = source.fec_modificacion,
# MAGIC         target.fec_cierre = source.fec_cierre,
# MAGIC         target.fec_pago_matricula = source.fec_pago_matricula,
# MAGIC         target.fecha_hora_anulacion = source.fecha_hora_anulacion,
# MAGIC         target.fecha_Modificacion_Lead = source.fecha_Modificacion_Lead,
# MAGIC         target.fecha_Modificacion_Oportunidad = source.fecha_Modificacion_Oportunidad,
# MAGIC         target.importe_matricula = source.importe_matricula,
# MAGIC         target.importe_descuento_matricula = source.importe_descuento_matricula,
# MAGIC         target.importe_neto_matricula = source.importe_neto_matricula,
# MAGIC         target.kpi_new_enrollent = source.kpi_new_enrollent,
# MAGIC         target.kpi_lead_neto = source.kpi_lead_neto,
# MAGIC         target.kpi_lead_bruto = source.kpi_lead_bruto,
# MAGIC         target.activo = source.activo,
# MAGIC         target.id_classlife = source.id_classlife,
# MAGIC         target.id_tipo_registro = source.id_tipo_registro,
# MAGIC         target.tipo_registro = source.tipo_registro,
# MAGIC         target.id_dim_propietario_lead = source.id_dim_propietario_lead,
# MAGIC         target.id_dim_programa = source.id_dim_programa,
# MAGIC         target.id_dim_producto = source.id_dim_producto,
# MAGIC         target.id_dim_utm_campaign = source.id_dim_utm_campaign,
# MAGIC         target.id_dim_utm_ad = source.id_dim_utm_ad,
# MAGIC         target.id_dim_utm_source = source.id_dim_utm_source,
# MAGIC         target.id_dim_nacionalidad = source.id_dim_nacionalidad,
# MAGIC         target.id_dim_tipo_formacion = source.id_dim_tipo_formacion,
# MAGIC         target.id_dim_tipo_negocio = source.id_dim_tipo_negocio,
# MAGIC         target.id_dim_modalidad = source.id_dim_modalidad,
# MAGIC         target.id_dim_institucion = source.id_dim_institucion,
# MAGIC         target.id_dim_sede = source.id_dim_sede,
# MAGIC         target.id_dim_pais = source.id_dim_pais,
# MAGIC         target.id_dim_estado_venta = source.id_dim_estado_venta,
# MAGIC         target.id_dim_etapa_venta = source.id_dim_etapa_venta,
# MAGIC         target.id_dim_motivo_perdida = source.id_dim_motivo_perdida,
# MAGIC         target.id_dim_vertical = source.id_dim_vertical,
# MAGIC         target.ETLupdatedDate = current_timestamp
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
# MAGIC         id_dim_programa, id_dim_producto, id_dim_utm_campaign, id_dim_utm_ad, id_dim_utm_source, 
# MAGIC         id_dim_nacionalidad, id_dim_tipo_formacion, id_dim_tipo_negocio, id_dim_modalidad, 
# MAGIC         id_dim_institucion, id_dim_sede, id_dim_pais, id_dim_estado_venta, id_dim_etapa_venta, 
# MAGIC         id_dim_motivo_perdida, id_dim_vertical, ETLcreatedDate, ETLupdatedDate
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
# MAGIC         source.id_dim_producto, source.id_dim_utm_campaign, source.id_dim_utm_ad, 
# MAGIC         source.id_dim_utm_source, source.id_dim_nacionalidad, source.id_dim_tipo_formacion, 
# MAGIC         source.id_dim_tipo_negocio, source.id_dim_modalidad, source.id_dim_institucion, 
# MAGIC         source.id_dim_sede, source.id_dim_pais, source.id_dim_estado_venta, source.id_dim_etapa_venta, 
# MAGIC         source.id_dim_motivo_perdida, source.id_dim_vertical, current_timestamp, current_timestamp
# MAGIC     );

# COMMAND ----------

# DBTITLE 1,Eliminar Registros Duplicados de Estados Anteriores
#%sql
#DELETE FROM silver_lakehouse.tablon_leads_and_deals
#WHERE id_tipo_registro IN (1,3)
#AND (
#    -- Si existe un estado 2 con la misma `cod_Lead`
#    EXISTS (
#        SELECT 1 FROM silver_lakehouse.tablon_leads_and_deals existing
#        WHERE existing.cod_Lead = tablon_leads_and_deals.cod_Lead
#        AND existing.id_tipo_registro = 2
#    )
#    OR 
#    -- Si existe un estado 2 con la misma `cod_Oportunidad`
#    EXISTS (
#        SELECT 1 FROM silver_lakehouse.tablon_leads_and_deals existing
#        WHERE existing.cod_Oportunidad = tablon_leads_and_deals.cod_Oportunidad
#        AND existing.id_tipo_registro = 2
#    )
#);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cod_lead, cod_oportunidad, COUNT(*)
# MAGIC FROM gold_lakehouse.fctventa
# MAGIC GROUP BY cod_lead, cod_oportunidad
# MAGIC HAVING COUNT(*) > 1;
