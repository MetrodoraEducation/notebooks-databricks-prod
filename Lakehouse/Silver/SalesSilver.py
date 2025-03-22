# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW odoo_view
# MAGIC     AS SELECT 
# MAGIC     date_closed as fec_cierre,
# MAGIC     sale_amount_total as importe_venta,
# MAGIC     email_from as email,
# MAGIC     contact_name as nombre_contacto,
# MAGIC     phone as telefono,
# MAGIC     create_date as fec_creacion,
# MAGIC     id as cod_venta,
# MAGIC     write_date as fec_modificacion,
# MAGIC     name as nombre,
# MAGIC     --user_value as propietario_lead,
# MAGIC     case when user_value = '' then 'n/a' else user_value end as propietario_lead,
# MAGIC     --stage_value as etapa_venta,
# MAGIC     case when stage_value='Won' then 'Ganada'
# MAGIC     when stage_value='Proposition' or stage_value='Negociación' then 'Interesado'
# MAGIC     when stage_value='New' or stage_value='Duplicado' then 'Sin asignar'
# MAGIC     when stage_value='Sin Contacto' or stage_value='Qualified' then 'Seguimiento'
# MAGIC     else 'n/a' end  as etapa_venta,
# MAGIC     probability as posibilidad_venta,
# MAGIC     --state_value as estado_venta,
# MAGIC     case when (lost_reason_value is null or lost_reason_value ='') and date_closed is not null then 'Cerrada'
# MAGIC     when (lost_reason_value is null or lost_reason_value ='') and date_closed is null then 'Abierta'
# MAGIC     when lost_reason_value is not null or lost_reason_value <>'' then 'Anulada'
# MAGIC     else 'n/a' end   as estado_venta,
# MAGIC     "n/a" as nombre_scoring,
# MAGIC     0 as puntos_scoring,
# MAGIC     --city as localidad,
# MAGIC     case  when (city is null or city ='') then 'n/a' else city end as localidad,
# MAGIC     0 as importe_descuento,
# MAGIC     0 as importe_descuento_matricula,
# MAGIC     x_curso_value as titulacion,
# MAGIC     date_conversion as fec_pago_matricula,
# MAGIC     0 as importe_matricula,
# MAGIC     --x_codmodalidad as modalidad, 
# MAGIC     x_modalidad_value as modalidad,
# MAGIC     --country_value as pais,
# MAGIC     case when country_value ='United States' then 'United States of America'
# MAGIC     --when country_value ='TAX MEXICO' then 'n/a' 
# MAGIC     when country_value ='Russian Federation' then 'Russia'
# MAGIC     when country_value not in (select name from silver_lakehouse.dim_pais) then 'n/a' else country_value end as pais,
# MAGIC     x_sede_value as sede,
# MAGIC     x_ga_campaign as campania,
# MAGIC     x_ga_source as origen_campania,
# MAGIC     --lost_reason_value as motivo_cierre,
# MAGIC     case  when (lost_reason_value is null or lost_reason_value ='') then 'n/a' else lost_reason_value end as motivo_cierre,
# MAGIC     processdate as fec_procesamiento,
# MAGIC     sourcesystem as sistema_origen,
# MAGIC     date_diff(date_conversion, create_date ) as tiempo_de_maduracion,
# MAGIC     "ISEP" as institucion,
# MAGIC     case when stage_value='Won' and date_closed is not null then 1 else 0 end as new_enrollent,
# MAGIC     case when stage_value='Duplicado' then 0 else 1 end as lead_neto,
# MAGIC     1 as activo
# MAGIC
# MAGIC          FROM silver_lakehouse.odoolead AS odoo_table where processdate> (select IFNULL(max(fec_procesamiento),'1900-01-01') from silver_lakehouse.sales where sistema_origen='Odoo')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW clientify_view
# MAGIC     AS SELECT 
# MAGIC     actual_closed_date as fec_cierre,
# MAGIC     amount as importe_venta,
# MAGIC     contact_email as email,
# MAGIC     contact_name as nombre_contacto,
# MAGIC     contact_phone as telefono,
# MAGIC     created as fec_creacion,
# MAGIC     id as cod_venta,
# MAGIC     modified as fec_modificacion,
# MAGIC     name as nombre,
# MAGIC     --owner_name as propietario_lead,
# MAGIC     case when owner_name = '' then 'n/a' else owner_name end as propietario_lead,
# MAGIC     --pipeline_stage_desc as etapa_venta,
# MAGIC     case when pipeline_stage_desc='Traslado a ERP' then 'Ganada'
# MAGIC     when pipeline_stage_desc='Seguimiento Interesado' then 'Interesado'
# MAGIC     when pipeline_stage_desc='Sin Gestionar' then 'Sin asignar'
# MAGIC     when pipeline_stage_desc='Seguimiento Primer Contacto' or pipeline_stage_desc='No contesta' or pipeline_stage_desc='Seguimiento Valorando' then 'Seguimiento'
# MAGIC     else 'n/a' end as etapa_venta,
# MAGIC     probability_desc as posibilidad_venta,
# MAGIC     --status_desc as estado_venta,
# MAGIC     case when status_desc='Won' then 'Cerrada'
# MAGIC     when status_desc='Open' then 'Abierta'
# MAGIC     when status_desc='Expired' or status_desc='Lost' then 'Anulada'
# MAGIC     else 'n/a' end  as estado_venta,
# MAGIC     custom_fields_byratings_rating as nombre_scoring,
# MAGIC     custom_fields_byratings_score as puntos_scoring,
# MAGIC     --custom_fields_ciudad as localidad,
# MAGIC     case  when (custom_fields_ciudad is null or custom_fields_ciudad ='') then 'n/a' else custom_fields_ciudad end as localidad,
# MAGIC     custom_fields_descuento as importe_descuento,
# MAGIC     custom_fields_descuento_matricula as importe_descuento_matricula,
# MAGIC     custom_fields_estudio as titulacion,
# MAGIC     custom_fields_fecha_inscripcion as fec_pago_matricula,
# MAGIC     custom_fields_matricula as importe_matricula,
# MAGIC     custom_fields_modalidad as modalidad,
# MAGIC     --custom_fields_pais as pais,
# MAGIC     case when custom_fields_pais='The Netherlands' then 'Netherlands' when custom_fields_pais='España' then 'Spain' when custom_fields_pais='Türkiye' then 'Turkey' when custom_fields_pais='United States' then 'United States of America' when custom_fields_pais not in (select name from silver_lakehouse.dim_pais) then 'n/a' else custom_fields_pais end as pais,
# MAGIC     custom_fields_sede as sede,
# MAGIC     custom_fields_utm_campaign_name as campania,
# MAGIC     custom_fields_utm_source as origen_campania,
# MAGIC     --lost_reason as motivo_cierre,
# MAGIC     case  when (lost_reason is null or lost_reason ='') then 'n/a' else lost_reason end as motivo_cierre,
# MAGIC     processdate as fec_procesamiento,
# MAGIC     sourcesystem as sistema_origen,
# MAGIC     date_diff(custom_fields_fecha_inscripcion, created ) as tiempo_de_maduracion,
# MAGIC     "CESIF" as institucion,
# MAGIC     --iif(isNull(fecha_inscripcion), 0, 1)
# MAGIC     case when isnull(custom_fields_fecha_inscripcion) then 0 else 1 end as new_enrollent,
# MAGIC     case when lost_reason in ('NV Datos erróneos', 'NV Duplicado', 'NV Busca empleo', 'NV Niño', 'NV Extranjero','NV Bot','NV Test','NV Lista Robinson')  then 0 else 1 end as lead_neto,
# MAGIC     case when lost_reason in ('NV Bot','NV Test') then 0 else 1 end as activo
# MAGIC
# MAGIC          FROM silver_lakehouse.clientifydeals AS clientify_table  where processdate> (select IFNULL(max(fec_procesamiento),'1900-01-01') from silver_lakehouse.sales where sistema_origen LIKE '%Clientify') --14/01/2025 - sourcesystem se ha cambiado de de = Clientify a LIKE %Clientify para que reconozca el Clientify CESIF, Clientify CIEP y Clientify FP

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sales_view
# MAGIC    AS SELECT * from clientify_view
# MAGIC  union select * from odoo_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE WITH SCHEMA EVOLUTION 
# MAGIC INTO silver_lakehouse.sales
# MAGIC USING sales_view 
# MAGIC ON silver_lakehouse.sales.cod_venta = sales_view.cod_venta and silver_lakehouse.sales.sistema_origen = sales_view.sistema_origen
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
