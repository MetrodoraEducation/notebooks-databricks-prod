# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW etapa_venta_sales_view AS 
# MAGIC     select lead_status as nombre_etapa
# MAGIC     from silver_lakehouse.zoholeads
# MAGIC     union 
# MAGIC     select etapa as nombre_etapa
# MAGIC     from silver_lakehouse.zohodeals
# MAGIC     union
# MAGIC     select lead_status as nombre_etapa
# MAGIC     from silver_lakehouse.zoholeads_38b
# MAGIC     union 
# MAGIC     select etapa as nombre_etapa
# MAGIC     from silver_lakehouse.zohodeals_38b;
# MAGIC
# MAGIC select * from etapa_venta_sales_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_etapa_venta AS target
# MAGIC USING (
# MAGIC     -- Solo devuelve datos si el registro -1 no existe
# MAGIC     SELECT 'n/a' AS nombre_etapa_venta
# MAGIC           ,'n/a' AS nombreEtapaVentaAgrupado
# MAGIC           ,0 AS esNE
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1 
# MAGIC         FROM gold_lakehouse.dim_etapa_venta 
# MAGIC         WHERE id_dim_etapa_venta = -1 
# MAGIC            OR orden_etapa = -1
# MAGIC     )
# MAGIC ) AS source
# MAGIC ON target.id_dim_etapa_venta = -1
# MAGIC OR target.orden_etapa = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_etapa_venta, nombreEtapaVentaAgrupado, esNE, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_etapa_venta, source.nombreEtapaVentaAgrupado, source.esNE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create view dim etapa Zoho's
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_etapa_venta_view AS
# MAGIC SELECT DISTINCT
# MAGIC     nombre_etapa AS nombreEtapaVenta,
# MAGIC     CASE 
# MAGIC         WHEN trim(upper(nombre_etapa)) IN ('NUEVO','SIN ASIGNAR') THEN 'Nuevo'
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('ASIGNADO', 'SIN GESTIONAR', 'CONTACTANDO') THEN 'Asignado'
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('CONTACTADO', 'SIN INFORMACIÓN') THEN 'Contactado'
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('CONVERTIDO', 'SEGUIMIENTO', 'VALORANDO') THEN 'Seguimiento'
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('INTERESADO', 'CITA', 'PROPUESTA ECONÓMICA') THEN 'Interesado'
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('PENDIENTE PAGO', 'PTE. DE PAGO') THEN 'Pendiente Pago'
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('PAGADO (NE)','PAGADO NE') THEN 'Pagado (NE)'
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('PENDIENTE DOCUMENTACIÓN', 'PTE. DOCUMENTACIÓN', 'PTE. REVISIÓN DE DOC.') THEN 'Pendiente Doc.'
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('MATRICULADO', 'GANADA', 'NEC') THEN 'Pagado (NEC)'
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('PERDIDO', 'NO INTERESADO') THEN 'Perdido'
# MAGIC         ELSE 'Desconocido'  -- En caso de valores no contemplados
# MAGIC     END AS nombreEtapaVentaAgrupado
# MAGIC     ,CASE 
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('PAGADO (NE)','PAGADO NE')  THEN 1
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('PENDIENTE DOCUMENTACIÓN', 'PTE. DOCUMENTACIÓN', 'PTE. REVISIÓN DE DOC.')  THEN 1
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('MATRICULADO', 'GANADA', 'NEC') THEN 1
# MAGIC         ELSE 0 
# MAGIC     END AS esNE,
# MAGIC     CASE 
# MAGIC         WHEN trim(upper(nombre_etapa)) IN ('NUEVO') THEN 1
# MAGIC         WHEN trim(upper(nombre_etapa)) IN ('SIN ASIGNAR') THEN 2    
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('SIN GESTIONAR') THEN 3
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('CONTACTANDO') THEN 4
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('CONTACTADO', 'SIN INFORMACIÓN') THEN 5
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('CONVERTIDO', 'SEGUIMIENTO', 'VALORANDO') THEN 6
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('INTERESADO') THEN 7
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('CITA') THEN 8
# MAGIC          WHEN trim(upper(nombre_etapa))  IN ('PROPUESTA ECONÓMICA') THEN 9
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('PENDIENTE PAGO', 'PTE. DE PAGO') THEN 20
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('PAGADO (NE)','PAGADO NE') THEN 25
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('PENDIENTE DOCUMENTACIÓN', 'PTE. DOCUMENTACIÓN', 'PTE. REVISIÓN DE DOC.') THEN 30
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('MATRICULADO') THEN 50
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('GANADA', 'NEC') THEN 70
# MAGIC         WHEN trim(upper(nombre_etapa))  IN ('PERDIDO', 'NO INTERESADO') THEN 90
# MAGIC         ELSE 99  -- En caso de valores no contemplados
# MAGIC     END as orden_etapa,
# MAGIC     CURRENT_TIMESTAMP AS ETLcreatedDate,
# MAGIC     CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM etapa_venta_sales_view
# MAGIC WHERE nombre_etapa <> '';
# MAGIC
# MAGIC select * from dim_etapa_venta_view;

# COMMAND ----------

# DBTITLE 1,Nueva Vista Temporal para Insertar con Secuencia Automática
# MAGIC %sql
# MAGIC -- Esta vista añade fila incremental para calcular id_dim_etapa_venta y orden_etapa automáticamente
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_etapa_venta_nuevas_etapas_view AS
# MAGIC WITH max_vals AS (
# MAGIC   SELECT 
# MAGIC     COALESCE(MAX(id_dim_etapa_venta), 0) AS max_id
# MAGIC     --,COALESCE(MAX(orden_etapa), 0) AS max_orden
# MAGIC   FROM gold_lakehouse.dim_etapa_venta
# MAGIC ),
# MAGIC nuevas AS (
# MAGIC   SELECT 
# MAGIC     source.*,
# MAGIC     ROW_NUMBER() OVER (ORDER BY source.nombreEtapaVenta) AS rn
# MAGIC   FROM dim_etapa_venta_view AS source
# MAGIC   LEFT ANTI JOIN gold_lakehouse.dim_etapa_venta AS tgt
# MAGIC     ON tgt.nombre_etapa_venta = source.nombreEtapaVenta
# MAGIC )
# MAGIC SELECT 
# MAGIC   nv.nombreEtapaVenta,
# MAGIC   nv.nombreEtapaVentaAgrupado,
# MAGIC   nv.esNE,
# MAGIC   mv.max_id + nv.rn AS id_dim_etapa_venta,
# MAGIC   nv.orden_etapa,--mv.max_orden + nv.rn AS orden_etapa,
# MAGIC   nv.ETLcreatedDate,
# MAGIC   nv.ETLupdatedDate
# MAGIC FROM nuevas nv
# MAGIC CROSS JOIN max_vals mv;

# COMMAND ----------

# DBTITLE 1,MERGE adaptado
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_etapa_venta AS target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     nombreEtapaVenta,
# MAGIC     nombreEtapaVentaAgrupado,
# MAGIC     esNE,
# MAGIC     id_dim_etapa_venta,
# MAGIC     orden_etapa,
# MAGIC     ETLcreatedDate,
# MAGIC     ETLupdatedDate
# MAGIC   FROM dim_etapa_venta_nuevas_etapas_view
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     nombreEtapaVenta,
# MAGIC     nombreEtapaVentaAgrupado,
# MAGIC     esNE,
# MAGIC     NULL AS id_dim_etapa_venta,
# MAGIC     orden_etapa,
# MAGIC     ETLcreatedDate,
# MAGIC     ETLupdatedDate
# MAGIC   FROM dim_etapa_venta_view
# MAGIC ) AS source
# MAGIC
# MAGIC ON target.nombre_etapa_venta = source.nombreEtapaVenta
# MAGIC
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET 
# MAGIC     target.nombreEtapaVentaAgrupado = source.nombreEtapaVentaAgrupado,
# MAGIC     target.esNE = source.esNE,
# MAGIC     target.orden_etapa = source.orden_etapa,
# MAGIC     target.ETLupdatedDate = source.ETLupdatedDate
# MAGIC
# MAGIC WHEN NOT MATCHED AND source.id_dim_etapa_venta IS NOT NULL THEN 
# MAGIC   INSERT (
# MAGIC     id_dim_etapa_venta,
# MAGIC     orden_etapa,
# MAGIC     nombre_etapa_venta,
# MAGIC     nombreEtapaVentaAgrupado,
# MAGIC     esNE,
# MAGIC     ETLcreatedDate,
# MAGIC     ETLupdatedDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.id_dim_etapa_venta,
# MAGIC     source.orden_etapa,
# MAGIC     source.nombreEtapaVenta,
# MAGIC     source.nombreEtapaVentaAgrupado,
# MAGIC     source.esNE,
# MAGIC     source.ETLcreatedDate,
# MAGIC     source.ETLupdatedDate
# MAGIC   );
# MAGIC
