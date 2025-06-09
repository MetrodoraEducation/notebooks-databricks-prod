# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silver_venta_view
# MAGIC     AS SELECT * FROM silver_lakehouse.sales where fec_procesamiento > (select IFNULL(max(fec_procesamiento),'1900-01-01') from gold_lakehouse.fct_venta);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW modalidad_mapeo_view AS
# MAGIC SELECT a.cod_venta, a.modalidad,
# MAGIC IFNULL(b.modalidad_norm, 'n/a') AS modalidad_norm
# MAGIC FROM silver_venta_view a
# MAGIC left JOIN  gold_lakehouse.mapeo_modalidad b
# MAGIC on  a.modalidad = b.modalidad;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW modalidad_norm_view AS
# MAGIC SELECT a.cod_venta, a.modalidad,a.modalidad_norm, b.id_dim_modalidad
# MAGIC FROM modalidad_mapeo_view a
# MAGIC left JOIN  gold_lakehouse.dim_modalidad b
# MAGIC on  a.modalidad_norm = b.nombre_modalidad

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sede_mapeo_view AS
# MAGIC SELECT a.cod_venta, a.sede,
# MAGIC IFNULL(b.sede_norm, 'n/a') AS sede_norm
# MAGIC FROM silver_venta_view a
# MAGIC left JOIN  gold_lakehouse.mapeo_sede b
# MAGIC on  a.sede = b.sede

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sede_norm_view AS
# MAGIC SELECT a.cod_venta, a.sede,a.sede_norm, b.id_dim_sede
# MAGIC FROM sede_mapeo_view a
# MAGIC left JOIN  gold_lakehouse.dim_sede b
# MAGIC on  a.sede_norm = b.nombre_sede

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW estudio_mapeo_view AS
# MAGIC SELECT a.cod_venta, a.titulacion, a.sistema_origen,
# MAGIC case when b.estudio_norm is null and a.sistema_origen='Clientify' then 'CFGEN'
# MAGIC  when b.estudio_norm is null and a.sistema_origen='Odoo' then 'IPGEN'
# MAGIC  else b.estudio_norm end as estudio_norm
# MAGIC
# MAGIC FROM silver_venta_view a
# MAGIC left JOIN  gold_lakehouse.mapeo_estudio b
# MAGIC on  a.titulacion = b.estudio

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW estudio_norm_view AS
# MAGIC SELECT a.cod_venta, a.titulacion, a.sistema_origen, a.estudio_norm, b.id_dim_estudio, c.id_dim_tipo_negocio, d.id_dim_tipo_formacion
# MAGIC FROM estudio_mapeo_view a
# MAGIC left JOIN  gold_lakehouse.dim_estudio b on  a.estudio_norm = b.cod_estudio
# MAGIC left JOIN  gold_lakehouse.dim_tipo_negocio c on  b.tipo_negocio_desc = c.tipo_negocio_desc
# MAGIC left JOIN  gold_lakehouse.dim_tipo_formacion d on  b.tipo_formacion_desc = d.tipo_formacion_desc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cruce con Ventas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fct_venta_view
# MAGIC     AS SELECT 
# MAGIC             a.cod_venta,
# MAGIC             a.nombre,
# MAGIC             a.email,
# MAGIC             a.telefono,
# MAGIC             a.nombre_contacto,
# MAGIC             COALESCE(b.id_dim_comercial, -1) as id_dim_propietario_lead,
# MAGIC             COALESCE(c.id_dim_origen_campania, -1) as id_dim_origen_campania,
# MAGIC             COALESCE(d.id_dim_campania, -1) as id_dim_campania,
# MAGIC             a.importe_venta,
# MAGIC             a.importe_descuento,
# MAGIC             (a.importe_venta-a.importe_descuento-a.importe_descuento_matricula) as importe_venta_neta,
# MAGIC             COALESCE(e.id_dim_estado_venta, -1) as id_dim_estado_venta,
# MAGIC             COALESCE(f.id_dim_etapa_venta, -1) as id_dim_etapa_venta,
# MAGIC             a.posibilidad_venta,
# MAGIC             to_date(a.fec_creacion) as fec_creacion,
# MAGIC             to_date(a.fec_modificacion) as fec_modificacion,
# MAGIC             to_date(a.fec_cierre) as fec_cierre,
# MAGIC             COALESCE(g.id_dim_modalidad, -1) as id_dim_modalidad,
# MAGIC             COALESCE(h.id_dim_institucion, -1) as id_dim_institucion,
# MAGIC             COALESCE(i.id_dim_sede, -1) as id_dim_sede,
# MAGIC             COALESCE(j.id, -1) as id_dim_pais,
# MAGIC             COALESCE(k.id_dim_estudio, -1) as id_dim_estudio,
# MAGIC             to_date(a.fec_pago_matricula) as fec_pago_matricula,
# MAGIC             a.importe_matricula, 
# MAGIC             a.importe_descuento_matricula,
# MAGIC             (a.importe_matricula-a.importe_descuento_matricula) as importe_neto_matricula,
# MAGIC             COALESCE(l.id_dim_localidad, -1) as id_dim_localidad,
# MAGIC             COALESCE(k.id_dim_tipo_formacion, -1) as id_dim_tipo_formacion,
# MAGIC             COALESCE(k.id_dim_tipo_negocio, -1) as id_dim_tipo_negocio,
# MAGIC             a.nombre_scoring,
# MAGIC             a.puntos_scoring,
# MAGIC             date_diff(a.fec_cierre, a.fec_creacion ) as dias_cierre,
# MAGIC             COALESCE(o.id_dim_motivo_cierre, -1) as id_dim_motivo_cierre,
# MAGIC             a.fec_procesamiento,
# MAGIC             a.sistema_origen,
# MAGIC             case when (a.tiempo_de_maduracion is null or a.tiempo_de_maduracion = '') then 0 else a.tiempo_de_maduracion end as tiempo_de_maduracion,
# MAGIC             a.new_enrollent,
# MAGIC             a.lead_neto,
# MAGIC             a.activo
# MAGIC     FROM silver_venta_view a 
# MAGIC     LEFT JOIN gold_lakehouse.dim_comercial b ON a.propietario_lead = b.nombre_comercial
# MAGIC     LEFT JOIN gold_lakehouse.dim_origen_campania c ON a.origen_campania = c.nombre_origen_campania --mapeo
# MAGIC     LEFT JOIN gold_lakehouse.dim_campania d ON a.campania = d.nombre_campania
# MAGIC     LEFT JOIN gold_lakehouse.dim_estado_venta e ON a.estado_venta = e.nombre_estado_venta
# MAGIC     LEFT JOIN gold_lakehouse.dim_etapa_venta f ON a.etapa_venta = f.nombre_etapa_venta
# MAGIC     LEFT JOIN modalidad_norm_view g ON a.cod_venta = g.cod_venta --mapeo
# MAGIC     LEFT JOIN gold_lakehouse.dim_institucion h ON a.institucion = h.nombre_institucion
# MAGIC     LEFT JOIN sede_norm_view i ON a.cod_venta = i.cod_venta --mapeo
# MAGIC     LEFT JOIN gold_lakehouse.dim_pais j ON a.pais = j.name
# MAGIC     LEFT JOIN estudio_norm_view k ON a.cod_venta  = k.cod_venta --mapeo
# MAGIC     LEFT JOIN gold_lakehouse.dim_localidad l ON a.localidad = l.nombre_localidad
# MAGIC     --LEFT JOIN gold_lakehouse.dim_tipo_formacion m ON a.id_dim_tipo_formacion = m.id_dim_tipo_formacion --mapeo
# MAGIC     --LEFT JOIN gold_lakehouse.dim_tipo_negocio n ON a.id_dim_tipo_negocio = n.id_dim_tipo_negocio --mapeo
# MAGIC     LEFT JOIN gold_lakehouse.dim_motivo_cierre o ON a.motivo_cierre = o.motivo_cierre;
# MAGIC

# COMMAND ----------

fct_venta_df = spark.sql("select distinct * from fct_venta_view")

# COMMAND ----------

fct_venta_df.createOrReplaceTempView("fct_venta_view")

# COMMAND ----------

fct_venta_df = fct_venta_df.dropDuplicates()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE WITH SCHEMA EVOLUTION 
# MAGIC INTO gold_lakehouse.fct_venta
# MAGIC USING fct_venta_view 
# MAGIC ON gold_lakehouse.fct_venta.cod_venta = fct_venta_view.cod_venta 
# MAGIC AND gold_lakehouse.fct_venta.sistema_origen = fct_venta_view.sistema_origen
# MAGIC AND gold_lakehouse.fct_venta.fec_procesamiento = fct_venta_view.fec_procesamiento  -- Agregar una columna adicional de cruce
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cod_venta, COUNT(*)
# MAGIC FROM fct_venta_view
# MAGIC GROUP BY cod_venta
# MAGIC HAVING COUNT(*) > 1;
