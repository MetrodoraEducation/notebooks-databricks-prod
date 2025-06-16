# Databricks notebook source
# MAGIC %md
# MAGIC ###FCT_MATRICULA

# COMMAND ----------

# DBTITLE 1,classlifeenrollments
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fct_matricula_temp AS
# MAGIC       SELECT 
# MAGIC              origen.id_Dim_Origen_SIS AS id_origen_SIS
# MAGIC             ,CONCAT(origen.codigo_Origen_SIS, enroll.enroll_id) AS cod_matricula
# MAGIC             ,COALESCE(dim_estudiante.id_dim_estudiante, -1) AS id_dim_estudiante
# MAGIC             ,COALESCE(programa.id_Dim_Programa, -1) AS id_dim_programa
# MAGIC             ,COALESCE(modalidad.id_dim_modalidad, -1) AS id_dim_modalidad
# MAGIC             ,COALESCE(institucion.id_dim_institucion, -1) AS id_dim_institucion
# MAGIC             ,COALESCE(sede.id_dim_sede, -1) AS id_dim_sede
# MAGIC             ,COALESCE(producto.id_Dim_Producto, -1) AS id_dim_producto
# MAGIC             ,COALESCE(formacion.id_dim_tipo_formacion, -1) AS id_dim_tipo_formacion
# MAGIC             ,COALESCE(tiponegocio.id_dim_tipo_negocio, -1) AS id_dim_tipo_negocio
# MAGIC             ,COALESCE(pais.id, -1) AS id_dim_pais
# MAGIC             ,enroll.year AS ano_curso
# MAGIC             ,CASE 
# MAGIC                     WHEN TRY_CAST(enroll.first_activate_enroll AS DATE) IS NOT NULL THEN TRY_CAST(enroll.first_activate_enroll AS DATE)
# MAGIC                     WHEN enroll.created_on IS NOT NULL THEN TRY_CAST(enroll.created_on AS DATE)
# MAGIC                     ELSE TRY_CAST(enroll.enroll_in AS DATE)
# MAGIC               END AS fec_matricula
# MAGIC             ,COALESCE(matricula.id_dim_estado_matricula, -1) AS id_dim_estado_matricula
# MAGIC             ,TRY_CAST('1900-01-01' AS DATE) AS fec_anulacion
# MAGIC             ,TRY_CAST('1900-01-01' AS DATE) AS fec_finalizacion
# MAGIC             ,0 AS nota_media
# MAGIC             ,enroll.codigo_promocion AS cod_descuento
# MAGIC             ,CASE 
# MAGIC                 WHEN fee_title_matricula IS NOT NULL OR fee_title_docencia IS NOT NULL 
# MAGIC                 THEN COALESCE(try_cast(fee_title_matricula AS DECIMAL(10, 2)), 0) + COALESCE(try_cast(fee_title_docencia AS DECIMAL(10, 2)), 0) 
# MAGIC                     - COALESCE(ABS(try_cast(suma_descuentos AS DECIMAL(10, 2))), 0)
# MAGIC                 ELSE 0 
# MAGIC             END AS importe_matricula
# MAGIC             ,ABS(enroll.suma_descuentos) AS importe_descuento
# MAGIC             ,try_cast(enroll.totalenroll AS DECIMAL(10, 2)) AS importe_cobros
# MAGIC             ,enroll.paymentmethod AS tipo_pago
# MAGIC             ,'FALTA en CL' AS edad_acceso
# MAGIC             ,TRY_CAST('1900-01-01' AS DATE) AS fec_ultimo_login_LMS
# MAGIC             ,enroll.zoho_deal_id AS zoho_deal_id
# MAGIC         FROM silver_lakehouse.ClasslifeEnrollments enroll
# MAGIC    LEFT JOIN gold_lakehouse.origenClasslife origen ON 1 = origen.id_Dim_Origen_SIS
# MAGIC    LEFT JOIN gold_lakehouse.dim_estudiante dim_estudiante ON dim_estudiante.cod_estudiante = CONCAT(origen.codigo_Origen_SIS, enroll.student_id)    
# MAGIC    LEFT JOIN gold_lakehouse.dim_producto producto ON enroll.enroll_group = NULLIF(producto.cod_Producto, '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_programa programa ON UPPER(producto.cod_Programa) = UPPER(programa.cod_Programa)
# MAGIC    LEFT JOIN gold_lakehouse.dim_modalidad modalidad ON trim(upper(producto.modalidad)) = trim(upper(modalidad.nombre_modalidad))
# MAGIC   -- Cambio para usar la nueva tabla de mapeo de instituciones silver_lakehouse.entidad_legal
# MAGIC   -- LEFT JOIN gold_lakehouse.dim_institucion institucion ON UPPER(producto.entidad_Legal) = NULLIF(UPPER(institucion.nombre_institucion), '')
# MAGIC    LEFT JOIN (select ent.entidad_legal, ins.id_dim_institucion from gold_lakehouse.dim_institucion ins
# MAGIC               left join silver_lakehouse.entidad_legal ent on ent.institucion = ins.nombre_institucion) institucion ON UPPER(producto.entidad_Legal) = NULLIF(UPPER(institucion.entidad_legal), '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_sede sede ON trim(upper(producto.sede)) = trim(upper(sede.nombre_sede))
# MAGIC    LEFT JOIN gold_lakehouse.dim_tipo_formacion formacion ON producto.tipo_Producto = NULLIF(formacion.tipo_formacion_desc, '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_tipo_negocio tiponegocio ON producto.tipo_Negocio = NULLIF(tiponegocio.tipo_negocio_desc, '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_pais pais ON UPPER(dim_estudiante.pais) = NULLIF(UPPER(pais.iso2), '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_estado_matricula matricula ON enroll.enroll_status_id = matricula.cod_estado_matricula;
# MAGIC
# MAGIC    --select * from fct_matricula_temp;

# COMMAND ----------

# DBTITLE 1,classlifeenrollments_931
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fct_matricula_931_temp AS
# MAGIC       SELECT 
# MAGIC              origen.id_Dim_Origen_SIS AS id_origen_SIS
# MAGIC             ,CONCAT(origen.codigo_Origen_SIS, enroll.enroll_id) AS cod_matricula
# MAGIC             ,COALESCE(dim_estudiante.id_dim_estudiante, -1) AS id_dim_estudiante
# MAGIC             ,COALESCE(programa.id_Dim_Programa, -1) AS id_dim_programa
# MAGIC             ,COALESCE(modalidad.id_dim_modalidad, -1) AS id_dim_modalidad
# MAGIC             ,COALESCE(institucion.id_dim_institucion, -1) AS id_dim_institucion
# MAGIC             ,COALESCE(sede.id_dim_sede, -1) AS id_dim_sede
# MAGIC             ,COALESCE(producto.id_Dim_Producto, -1) AS id_dim_producto
# MAGIC             ,COALESCE(formacion.id_dim_tipo_formacion, -1) AS id_dim_tipo_formacion
# MAGIC             ,COALESCE(tiponegocio.id_dim_tipo_negocio, -1) AS id_dim_tipo_negocio
# MAGIC             ,COALESCE(pais.id, -1) AS id_dim_pais
# MAGIC             ,enroll.year AS ano_curso
# MAGIC             --,enroll.enroll_in AS fec_matricula--
# MAGIC             ,CASE 
# MAGIC                   WHEN TRY_CAST(enroll.first_activate_enroll AS DATE) IS NOT NULL THEN TRY_CAST(enroll.first_activate_enroll AS DATE)
# MAGIC                   WHEN enroll.created_on IS NOT NULL THEN TRY_CAST(enroll.created_on AS DATE) --enroll_in
# MAGIC                   ELSE TRY_CAST(enroll.enroll_in AS DATE)
# MAGIC             END AS fec_matricula
# MAGIC             ,COALESCE(matricula.id_dim_estado_matricula, -1) AS id_dim_estado_matricula
# MAGIC             ,TRY_CAST('1900-01-01' AS DATE) AS fec_anulacion
# MAGIC             ,TRY_CAST('1900-01-01' AS DATE) AS fec_finalizacion
# MAGIC             ,0 AS nota_media
# MAGIC             ,'CODIGODUMMY' AS cod_descuento--,enroll.codigo_promocion AS cod_descuento
# MAGIC             --,0 AS importe_matricula
# MAGIC             ,CASE 
# MAGIC                 WHEN enroll.importe_matricula IS NOT NULL OR enroll.importe_docencia IS NOT NULL 
# MAGIC                 THEN COALESCE(try_cast(enroll.importe_matricula AS DECIMAL(10, 2)), 0) + COALESCE(try_cast(enroll.importe_docencia AS DECIMAL(10, 2)), 0) 
# MAGIC                     - COALESCE(ABS(try_cast(enroll.suma_descuentos AS DECIMAL(10, 2))), 0)
# MAGIC                 ELSE 0 
# MAGIC             END AS importe_matricula
# MAGIC             ,ABS(enroll.suma_descuentos) AS importe_descuento
# MAGIC             ,COALESCE(try_cast(enroll.total_fees AS DECIMAL(10, 2)), 0) AS importe_cobros--,try_cast(enroll.totalenroll AS DECIMAL(10, 2)) AS importe_cobros
# MAGIC             ,'PAGODUMMY' AS tipo_pago--,enroll.paymentmethod AS tipo_pago
# MAGIC             ,'FALTA en CL' AS edad_acceso
# MAGIC             ,TRY_CAST('1900-01-01' AS DATE) AS fec_ultimo_login_LMS
# MAGIC             ,enroll.zoho_deal_id AS zoho_deal_id--,enroll.zoho_deal_id AS zoho_deal_id
# MAGIC         FROM silver_lakehouse.classlifeenrollments_931 enroll
# MAGIC    LEFT JOIN gold_lakehouse.origenClasslife origen ON 2 = origen.id_Dim_Origen_SIS
# MAGIC    LEFT JOIN gold_lakehouse.dim_estudiante dim_estudiante ON dim_estudiante.cod_estudiante = CONCAT(origen.codigo_Origen_SIS, enroll.student_id)    
# MAGIC    LEFT JOIN gold_lakehouse.dim_producto producto ON enroll.enroll_group = NULLIF(producto.cod_Producto, '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_programa programa ON UPPER(producto.cod_Programa) = UPPER(programa.cod_Programa)
# MAGIC    LEFT JOIN gold_lakehouse.dim_modalidad modalidad ON trim(upper(producto.modalidad)) = trim(upper(modalidad.nombre_modalidad))
# MAGIC     -- Cambio para usar la nueva tabla de mapeo de instituciones silver_lakehouse.entidad_legal
# MAGIC    LEFT JOIN (select ent.entidad_legal, ins.id_dim_institucion from gold_lakehouse.dim_institucion ins
# MAGIC               left join silver_lakehouse.entidad_legal ent on ent.institucion = ins.nombre_institucion) institucion ON UPPER(producto.entidad_Legal) = NULLIF(UPPER(institucion.entidad_legal), '')
# MAGIC    --LEFT JOIN gold_lakehouse.dim_institucion institucion ON UPPER(producto.entidad_Legal) = NULLIF(UPPER(institucion.nombre_institucion), '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_sede sede ON trim(upper(producto.sede)) = trim(upper(sede.nombre_sede))
# MAGIC    LEFT JOIN gold_lakehouse.dim_tipo_formacion formacion ON producto.tipo_Producto = NULLIF(formacion.tipo_formacion_desc, '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_tipo_negocio tiponegocio ON producto.tipo_Negocio = NULLIF(tiponegocio.tipo_negocio_desc, '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_pais pais ON UPPER(dim_estudiante.pais) = NULLIF(UPPER(pais.iso2), '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_estado_matricula matricula ON enroll.enroll_status_id = matricula.cod_estado_matricula;
# MAGIC
# MAGIC    --select * from fct_matricula_931_temp;

# COMMAND ----------

# DBTITLE 1,Union View
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fct_matricula_unificada AS
# MAGIC SELECT * FROM fct_matricula_temp
# MAGIC UNION ALL
# MAGIC SELECT * FROM fct_matricula_931_temp;
# MAGIC
# MAGIC --select * from fct_matricula_unificada;

# COMMAND ----------

# DBTITLE 1,Deleted duplicates if exists
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fct_matricula_unique_temp AS
# MAGIC SELECT * 
# MAGIC FROM (
# MAGIC     SELECT *, 
# MAGIC            ROW_NUMBER() OVER (
# MAGIC                PARTITION BY cod_matricula 
# MAGIC                ORDER BY fec_matricula DESC
# MAGIC            ) AS rn
# MAGIC     FROM fct_matricula_unificada
# MAGIC ) AS ranked
# MAGIC WHERE rn = 1;
# MAGIC
# MAGIC --select * from fct_matricula_unique_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ Insertar nuevos valores sin duplicar registros
# MAGIC MERGE INTO gold_lakehouse.fct_matricula AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT 
# MAGIC         id_origen_SIS, cod_matricula, id_dim_estudiante, id_dim_programa, id_dim_modalidad,
# MAGIC         id_dim_institucion, id_dim_sede, id_dim_producto, id_dim_tipo_formacion,
# MAGIC         id_dim_tipo_negocio, id_dim_pais, ano_curso, fec_matricula, id_dim_estado_matricula,
# MAGIC         fec_anulacion, fec_finalizacion, nota_media, cod_descuento, importe_matricula,
# MAGIC         importe_descuento, try_cast(importe_cobros as DECIMAL(10,2)), tipo_pago, edad_acceso, fec_ultimo_login_LMS, zoho_deal_id
# MAGIC     FROM fct_matricula_unique_temp
# MAGIC ) AS source
# MAGIC ON target.cod_matricula = source.cod_matricula
# MAGIC WHEN MATCHED AND (
# MAGIC     target.id_origen_SIS IS DISTINCT FROM source.id_origen_SIS OR
# MAGIC     target.id_dim_estudiante IS DISTINCT FROM source.id_dim_estudiante OR
# MAGIC     target.id_dim_programa IS DISTINCT FROM source.id_dim_programa OR
# MAGIC     target.id_dim_modalidad IS DISTINCT FROM source.id_dim_modalidad OR
# MAGIC     target.id_dim_institucion IS DISTINCT FROM source.id_dim_institucion OR
# MAGIC     target.id_dim_sede IS DISTINCT FROM source.id_dim_sede OR
# MAGIC     target.id_dim_producto IS DISTINCT FROM source.id_dim_producto OR
# MAGIC     target.id_dim_tipo_formacion IS DISTINCT FROM source.id_dim_tipo_formacion OR
# MAGIC     target.id_dim_tipo_negocio IS DISTINCT FROM source.id_dim_tipo_negocio OR
# MAGIC     target.id_dim_pais IS DISTINCT FROM source.id_dim_pais OR
# MAGIC     target.ano_curso IS DISTINCT FROM source.ano_curso OR
# MAGIC     target.fec_matricula IS DISTINCT FROM source.fec_matricula OR
# MAGIC     target.id_dim_estado_matricula IS DISTINCT FROM source.id_dim_estado_matricula OR
# MAGIC     target.fec_anulacion IS DISTINCT FROM source.fec_anulacion OR
# MAGIC     target.fec_finalizacion IS DISTINCT FROM source.fec_finalizacion OR
# MAGIC     target.nota_media IS DISTINCT FROM source.nota_media OR
# MAGIC     target.cod_descuento IS DISTINCT FROM source.cod_descuento OR
# MAGIC     target.importe_matricula IS DISTINCT FROM source.importe_matricula OR
# MAGIC     target.importe_descuento IS DISTINCT FROM source.importe_descuento OR
# MAGIC     target.importe_cobros IS DISTINCT FROM source.importe_cobros OR
# MAGIC     target.tipo_pago IS DISTINCT FROM source.tipo_pago OR
# MAGIC     target.edad_acceso IS DISTINCT FROM source.edad_acceso OR
# MAGIC     target.fec_ultimo_login_LMS IS DISTINCT FROM source.fec_ultimo_login_LMS
# MAGIC ) THEN 
# MAGIC     UPDATE SET
# MAGIC         target.id_origen_SIS = source.id_origen_SIS,
# MAGIC         target.id_dim_estudiante = source.id_dim_estudiante,
# MAGIC         target.id_dim_programa = source.id_dim_programa,
# MAGIC         target.id_dim_modalidad = source.id_dim_modalidad,
# MAGIC         target.id_dim_institucion = source.id_dim_institucion,
# MAGIC         target.id_dim_sede = source.id_dim_sede,
# MAGIC         target.id_dim_producto = source.id_dim_producto,
# MAGIC         target.id_dim_tipo_formacion = source.id_dim_tipo_formacion,
# MAGIC         target.id_dim_tipo_negocio = source.id_dim_tipo_negocio,
# MAGIC         target.id_dim_pais = source.id_dim_pais,
# MAGIC         target.ano_curso = source.ano_curso,
# MAGIC         target.fec_matricula = source.fec_matricula,
# MAGIC         target.id_dim_estado_matricula = source.id_dim_estado_matricula,
# MAGIC         target.fec_anulacion = source.fec_anulacion,
# MAGIC         target.fec_finalizacion = source.fec_finalizacion,
# MAGIC         target.nota_media = source.nota_media,
# MAGIC         target.cod_descuento = source.cod_descuento,
# MAGIC         target.importe_matricula = source.importe_matricula,
# MAGIC         target.importe_descuento = source.importe_descuento,
# MAGIC         target.importe_cobros = source.importe_cobros,
# MAGIC         target.tipo_pago = source.tipo_pago,
# MAGIC         target.edad_acceso = source.edad_acceso,
# MAGIC         target.fec_ultimo_login_LMS = source.fec_ultimo_login_LMS,
# MAGIC         target.zoho_deal_id = source.zoho_deal_id,
# MAGIC         target.ETLupdatedDate = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         id_origen_SIS, cod_matricula, id_dim_estudiante, id_dim_programa, id_dim_modalidad,
# MAGIC         id_dim_institucion, id_dim_sede, id_dim_producto, id_dim_tipo_formacion,
# MAGIC         id_dim_tipo_negocio, id_dim_pais, ano_curso, fec_matricula, id_dim_estado_matricula,
# MAGIC         fec_anulacion, fec_finalizacion, nota_media, cod_descuento, importe_matricula,
# MAGIC         importe_descuento, importe_cobros, tipo_pago, edad_acceso, fec_ultimo_login_LMS, zoho_deal_id, ETLcreatedDate, ETLupdatedDate
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.id_origen_SIS, source.cod_matricula, source.id_dim_estudiante, source.id_dim_programa,
# MAGIC         source.id_dim_modalidad, source.id_dim_institucion, source.id_dim_sede, source.id_dim_producto,
# MAGIC         source.id_dim_tipo_formacion, source.id_dim_tipo_negocio, source.id_dim_pais, source.ano_curso,
# MAGIC         source.fec_matricula, source.id_dim_estado_matricula, source.fec_anulacion, source.fec_finalizacion,
# MAGIC         source.nota_media, source.cod_descuento, source.importe_matricula, source.importe_descuento, source.importe_cobros,
# MAGIC         source.tipo_pago, source.edad_acceso, source.fec_ultimo_login_LMS, source.zoho_deal_id, current_timestamp(), current_timestamp()
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cod_matricula, COUNT(*)
# MAGIC FROM gold_lakehouse.fct_matricula
# MAGIC GROUP BY cod_matricula
# MAGIC HAVING COUNT(*) > 1;
