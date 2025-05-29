# Databricks notebook source
# MAGIC %md
# MAGIC ###FCT_RECIBOS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fct_recibos_temp AS
# MAGIC SELECT 
# MAGIC              origen.id_Dim_Origen_SIS AS id_origen_SIS
# MAGIC             ,CONCAT(origen.codigo_Origen_SIS, receipts.receipt_id) AS cod_recibo
# MAGIC             ,CONCAT(origen.codigo_Origen_SIS, enrolladmis.enroll_id) AS cod_matricula
# MAGIC             ,COALESCE(concepto_cobro.id_dim_concepto_cobro, -1) AS id_dim_concepto_cobro
# MAGIC             ,receipts.emission_date AS fecha_emision
# MAGIC             ,receipts.expiry_date AS fecha_vencimiento
# MAGIC             ,receipts.collection_date AS fecha_pago
# MAGIC             ,CASE WHEN receipts.collection_date IS NULL THEN 'Pendiente'
# MAGIC                   WHEN receipts.collection_date IS NOT NULL THEN 'Pagado'
# MAGIC                   ELSE 'No aplica' 
# MAGIC              END estado
# MAGIC             ,receipts.receipt_total AS importe_recibo
# MAGIC             ,CASE WHEN receipts.invoice_id IS NULL or receipts.invoice_id = '0' THEN 'No'
# MAGIC                   ELSE 'Si'
# MAGIC              END tiene_factura
# MAGIC             ,receipts.payment_method AS forma_pago
# MAGIC             ,COALESCE(dim_estudiante.id_dim_estudiante, -1) AS id_dim_estudiante
# MAGIC             ,COALESCE(producto.id_Dim_Producto, -1) AS id_dim_producto
# MAGIC             ,COALESCE(programa.id_Dim_Programa, -1) AS id_dim_programa
# MAGIC             ,COALESCE(modalidad.id_dim_modalidad, -1) AS id_dim_modalidad
# MAGIC             ,COALESCE(institucion.id_dim_institucion, -1) AS id_dim_institucion
# MAGIC             ,COALESCE(sede.id_dim_sede, -1) AS id_dim_sede
# MAGIC             ,COALESCE(formacion.id_dim_tipo_formacion, -1) AS id_dim_tipo_formacion
# MAGIC             ,COALESCE(tiponegocio.id_dim_tipo_negocio, -1) AS id_dim_tipo_negocio
# MAGIC             ,CASE WHEN UPPER(producto.modalidad) = 'ONLINE' THEN fctmatricula.fec_matricula
# MAGIC                   WHEN concepto_cobro.tipo_reparto = 0 OR try_cast(producto.Fecha_Inicio_Reconocimiento AS DATE) IS NULL 
# MAGIC                   THEN fctmatricula.fec_matricula
# MAGIC                   ELSE producto.Fecha_Inicio_Reconocimiento
# MAGIC               END fec_inicio_reconocimiento
# MAGIC             ,CASE WHEN UPPER(producto.modalidad) = 'ONLINE' THEN fctmatricula.fec_matricula
# MAGIC                   WHEN concepto_cobro.tipo_reparto = 0 THEN fctmatricula.fec_matricula
# MAGIC                   WHEN try_cast(producto.Fecha_Inicio_Reconocimiento AS DATE) IS NULL THEN fctmatricula.fec_matricula + producto.meses_Duracion
# MAGIC                   ELSE producto.Fecha_Fin_Reconocimiento
# MAGIC               END fec_fin_reconocimiento
# MAGIC         FROM silver_lakehouse.ClasslifeReceipts receipts
# MAGIC    LEFT JOIN gold_lakehouse.origenClasslife origen ON 1 = origen.id_Dim_Origen_SIS
# MAGIC    LEFT JOIN gold_lakehouse.dim_concepto_cobro concepto_cobro ON receipts.receipt_concept = concepto_cobro.concepto
# MAGIC    FULL OUTER JOIN (SELECT COALESCE(enroll.enroll_group, admissions.enroll_group) AS enroll_group
# MAGIC                           ,COALESCE(enroll.enroll_id,admissions.id) AS enroll_id
# MAGIC                       FROM silver_lakehouse.classlifeenrollments enroll
# MAGIC                       FULL OUTER JOIN  silver_lakehouse.ClasslifeAdmissions admissions  
# MAGIC                         ON enroll.enroll_id = admissions.id) enrolladmis
# MAGIC                   ON receipts.enroll_id = enrolladmis.enroll_id
# MAGIC    LEFT JOIN gold_lakehouse.dim_estudiante dim_estudiante ON dim_estudiante.cod_estudiante = CONCAT(origen.codigo_Origen_SIS, receipts.student_id)    
# MAGIC    LEFT JOIN gold_lakehouse.dim_producto producto ON NULLIF(producto.cod_Producto, '') = enrolladmis.enroll_group
# MAGIC    LEFT JOIN gold_lakehouse.fct_matricula fctmatricula ON NULLIF(fctmatricula.cod_matricula, '') = CONCAT(origen.codigo_Origen_SIS, receipts.enroll_id)
# MAGIC    LEFT JOIN gold_lakehouse.dim_programa programa ON UPPER(producto.cod_Programa) = UPPER(programa.cod_Programa)
# MAGIC    LEFT JOIN gold_lakehouse.dim_modalidad modalidad ON SUBSTRING(enrolladmis.enroll_group, 18, 1) = SUBSTRING(modalidad.nombre_modalidad,1,1)
# MAGIC    LEFT JOIN gold_lakehouse.dim_institucion institucion ON UPPER(producto.entidad_Legal) = NULLIF(UPPER(institucion.nombre_institucion), '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_sede sede ON SUBSTRING(enrolladmis.enroll_group, 20, 3) = NULLIF(sede.codigo_sede, '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_tipo_formacion formacion ON producto.tipo_Producto = NULLIF(formacion.tipo_formacion_desc, '')
# MAGIC    LEFT JOIN gold_lakehouse.dim_tipo_negocio tiponegocio ON producto.tipo_Negocio = NULLIF(tiponegocio.tipo_negocio_desc, '')
# MAGIC    where origen.id_Dim_Origen_SIS IS NOT NULL;
# MAGIC
# MAGIC SELECT * FROM fct_recibos_temp; 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fct_recibos_temp_1 AS
# MAGIC SELECT receipts.*
# MAGIC        , CASE
# MAGIC             WHEN fec_inicio_reconocimiento IS NULL OR fec_fin_reconocimiento IS NULL THEN NULL
# MAGIC             ELSE
# MAGIC               FLOOR(DATEDIFF(day, fec_inicio_reconocimiento, fec_fin_reconocimiento) / 30) + 1
# MAGIC           END AS meses_reconocimiento
# MAGIC       ,receipts.importe_recibo / meses_reconocimiento AS importe_Mensual_Reconocimiento
# MAGIC   FROM fct_recibos_temp receipts;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fct_recibos_temp_deduplicado AS
# MAGIC WITH cte_deduplicados AS (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC                PARTITION BY cod_recibo ORDER BY fecha_emision DESC
# MAGIC            ) AS rn
# MAGIC     FROM fct_recibos_temp_1
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM cte_deduplicados
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cod_recibo, COUNT(*)
# MAGIC FROM fct_recibos_temp_deduplicado
# MAGIC GROUP BY cod_recibo
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.fct_recibos AS tgt
# MAGIC USING fct_recibos_temp_deduplicado AS src
# MAGIC ON tgt.cod_recibo = src.cod_recibo
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     tgt.id_dim_concepto_cobro <> src.id_dim_concepto_cobro OR
# MAGIC     tgt.cod_matricula <> src.cod_matricula OR
# MAGIC     tgt.fecha_vencimiento <> src.fecha_vencimiento OR
# MAGIC     tgt.fecha_pago <> src.fecha_pago OR
# MAGIC     tgt.estado <> src.estado OR
# MAGIC     tgt.importe_recibo <> src.importe_recibo OR
# MAGIC     tgt.tiene_factura <> src.tiene_factura OR
# MAGIC     tgt.forma_pago <> src.forma_pago OR
# MAGIC     tgt.id_dim_producto <> src.id_dim_producto OR
# MAGIC     tgt.id_dim_programa <> src.id_dim_programa OR
# MAGIC     tgt.id_dim_modalidad <> src.id_dim_modalidad OR
# MAGIC     tgt.id_dim_institucion <> src.id_dim_institucion OR
# MAGIC     tgt.id_dim_sede <> src.id_dim_sede OR
# MAGIC     tgt.id_dim_tipo_formacion <> src.id_dim_tipo_formacion OR
# MAGIC     tgt.id_dim_tipo_negocio <> src.id_dim_tipo_negocio OR
# MAGIC     tgt.fec_inicio_reconocimiento IS DISTINCT FROM src.fec_inicio_reconocimiento OR
# MAGIC     tgt.fec_fin_reconocimiento IS DISTINCT FROM src.fec_fin_reconocimiento OR
# MAGIC     tgt.meses_reconocimiento IS DISTINCT FROM src.meses_reconocimiento OR
# MAGIC     tgt.importe_Mensual_Reconocimiento IS DISTINCT FROM src.importe_Mensual_Reconocimiento
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     tgt.id_dim_concepto_cobro = src.id_dim_concepto_cobro,
# MAGIC     tgt.cod_matricula = src.cod_matricula,
# MAGIC     tgt.fecha_vencimiento = src.fecha_vencimiento,
# MAGIC     tgt.fecha_pago = src.fecha_pago,
# MAGIC     tgt.estado = src.estado,
# MAGIC     tgt.importe_recibo = src.importe_recibo,
# MAGIC     tgt.tiene_factura = src.tiene_factura,
# MAGIC     tgt.forma_pago = src.forma_pago,
# MAGIC     tgt.id_dim_producto = src.id_dim_producto,
# MAGIC     tgt.id_dim_programa = src.id_dim_programa,
# MAGIC     tgt.id_dim_modalidad = src.id_dim_modalidad,
# MAGIC     tgt.id_dim_institucion = src.id_dim_institucion,
# MAGIC     tgt.id_dim_sede = src.id_dim_sede,
# MAGIC     tgt.id_dim_tipo_formacion = src.id_dim_tipo_formacion,
# MAGIC     tgt.id_dim_tipo_negocio = src.id_dim_tipo_negocio,
# MAGIC     tgt.fec_inicio_reconocimiento = src.fec_inicio_reconocimiento,
# MAGIC     tgt.fec_fin_reconocimiento = src.fec_fin_reconocimiento,
# MAGIC     tgt.meses_reconocimiento = src.meses_reconocimiento,
# MAGIC     tgt.importe_Mensual_Reconocimiento = src.importe_Mensual_Reconocimiento,
# MAGIC     tgt.ETLupdatedDate = CURRENT_TIMESTAMP()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     id_origen_SIS, cod_recibo, id_dim_concepto_cobro, fecha_emision, fecha_vencimiento, fecha_pago, estado, importe_recibo, 
# MAGIC     tiene_factura, forma_pago, id_dim_estudiante, id_dim_producto, cod_matricula, id_dim_programa, id_dim_modalidad, id_dim_institucion, 
# MAGIC     id_dim_sede, id_dim_tipo_formacion, id_dim_tipo_negocio, fec_inicio_reconocimiento, fec_fin_reconocimiento, meses_reconocimiento, 
# MAGIC     importe_Mensual_Reconocimiento, ETLcreatedDate, ETLupdatedDate
# MAGIC )
# MAGIC VALUES (
# MAGIC     src.id_origen_SIS, src.cod_recibo, src.id_dim_concepto_cobro, src.fecha_emision, src.fecha_vencimiento, src.fecha_pago, src.estado, src.importe_recibo, 
# MAGIC     src.tiene_factura, src.forma_pago, src.id_dim_estudiante, src.id_dim_producto, src.cod_matricula, src.id_dim_programa, src.id_dim_modalidad, src.id_dim_institucion, 
# MAGIC     src.id_dim_sede, src.id_dim_tipo_formacion, src.id_dim_tipo_negocio, src.fec_inicio_reconocimiento, src.fec_fin_reconocimiento, src.meses_reconocimiento, 
# MAGIC     src.importe_Mensual_Reconocimiento, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cod_recibo, COUNT(*)
# MAGIC FROM gold_lakehouse.fct_recibos
# MAGIC GROUP BY cod_recibo
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

#%sql
#DELETE
#FROM gold_lakehouse.fct_recibos
#WHERE cod_recibo IN (
#    'CLFC2660', 'CLFC1921', 'CLFC1920', 'CLFC1450', 'CLFC1742', 'CLFC1770',
#    'CLFC1500', 'CLFC1501', 'CLFC1830', 'CLFC1299', 'CLFC1829', 'CLFC1784',
#    'CLFC1733', 'CLFC1840', 'CLFC1839', 'CLFC1460', 'CLFC1470', 'CLFC1941',
#    'CLFC1940', 'CLFC1942', 'CLFC1967', 'CLFC2035', 'CLFC2034', 'CLFC2108',
#    'CLFC2107', 'CLFC2067', 'CLFC2069', 'CLFC2086', 'CLFC2242', 'CLFC2243',
#    'CLFC2410', 'CLFC2405', 'CLFC2406', 'CLFC2668', 'CLFC1975', 'CLFC1998',
#    'CLFC1974', 'CLFC2033', 'CLFC2031', 'CLFC2032', 'CLFC2109', 'CLFC2149',
#    'CLFC2139', 'CLFC2116', 'CLFC2150', 'CLFC2138', 'CLFC2115', 'CLFC2157',
#    'CLFC2208', 'CLFC2220', 'CLFC2277', 'CLFC2278', 'CLFC2295', 'CLFC2330',
#    'CLFC2332', 'CLFC2333', 'CLFC2442', 'CLFC2491', 'CLFC2490', 'CLFC1727',
#    'CLFC1752', 'CLFC1728', 'CLFC1032', 'CLFC1966', 'CLFC1965', 'CLFC2068',
#    'CLFC1362'
#);
