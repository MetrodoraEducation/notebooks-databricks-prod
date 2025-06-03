# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW etapa_venta_sales_view AS 
# MAGIC     SELECT 
# MAGIC             DISTINCT 
# MAGIC                      etapa_venta as nombre_etapa_venta
# MAGIC                     ,CASE
# MAGIC                         WHEN etapa_venta = 'Ganada' THEN 'Matriculado'   
# MAGIC                         WHEN etapa_venta = 'Interesado' THEN 'Interesado'
# MAGIC                         WHEN etapa_venta IN ('Seguimiento', 'Valorando', 'Cita') THEN 'Seguimiento'
# MAGIC                         WHEN etapa_venta = 'Sin asignar' THEN 'Desconocido'
# MAGIC                     ELSE etapa_venta
# MAGIC                     END AS nombreEtapaVentaAgrupado
# MAGIC                     ,CASE 
# MAGIC                         WHEN etapa_venta LIKE 'Nuevo%' THEN 0
# MAGIC                         WHEN etapa_venta LIKE 'Asignado%' THEN 0
# MAGIC                         WHEN etapa_venta LIKE 'Sin gestionar%' THEN 0
# MAGIC                         WHEN etapa_venta LIKE 'Contactando%' THEN 0
# MAGIC                         WHEN etapa_venta LIKE 'Contactado%' THEN 0
# MAGIC                         WHEN etapa_venta LIKE 'Sin información%' THEN 0
# MAGIC                         WHEN etapa_venta LIKE 'Seguimiento%' THEN 0
# MAGIC                         WHEN etapa_venta LIKE 'Valorando%' THEN 0
# MAGIC                         WHEN etapa_venta LIKE 'Cita%' THEN 0
# MAGIC                         WHEN etapa_venta LIKE 'Interesado%' THEN 0
# MAGIC                         WHEN etapa_venta LIKE 'Pendiente de pago%' THEN 0
# MAGIC                         WHEN etapa_venta LIKE 'Ganada%' THEN 1
# MAGIC                         WHEN etapa_venta LIKE 'Pagado (NE)%' THEN 1
# MAGIC                         WHEN etapa_venta LIKE 'Pendiente Entrevista%' THEN 1
# MAGIC                         WHEN etapa_venta LIKE 'Pendiente prueba%' THEN 1
# MAGIC                         WHEN etapa_venta LIKE 'Pendiente documentación%' THEN 1
# MAGIC                         WHEN etapa_venta LIKE 'Matriculado%' THEN 1
# MAGIC                         WHEN etapa_venta LIKE 'Pagado (NEC)%' THEN 1
# MAGIC                         ELSE 0 
# MAGIC                     END AS esNE
# MAGIC       FROM silver_lakehouse.sales
# MAGIC      WHERE etapa_venta <> 'n/a';

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_etapa_venta AS target
# MAGIC USING (
# MAGIC     -- Solo devuelve datos si el registro -1 no existe
# MAGIC     SELECT 'n/a' AS nombre_etapa_venta
# MAGIC           ,'n/a' AS nombreEtapaVentaAgrupado
# MAGIC           ,-1 AS esNE
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

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_etapa_venta AS target
# MAGIC USING etapa_venta_sales_view AS source
# MAGIC ON UPPER(target.nombre_etapa_venta) = UPPER(source.nombre_etapa_venta)
# MAGIC AND target.id_dim_etapa_venta != -1  -- 🔹 Evita afectar el registro `-1`
# MAGIC
# MAGIC -- 🔹 **Si ya existe, lo actualiza (si aplica)**
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.nombre_etapa_venta = source.nombre_etapa_venta,
# MAGIC         target.nombreEtapaVentaAgrupado = source.nombreEtapaVentaAgrupado,
# MAGIC         target.esNE = source.esNE
# MAGIC
# MAGIC -- 🔹 **Si no existe, lo inserta**
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_etapa_venta, nombreEtapaVentaAgrupado, esNE, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_etapa_venta, source.nombreEtapaVentaAgrupado, source.esNE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

# COMMAND ----------

# DBTITLE 1,Create view dim etapa Zoho's
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_etapa_venta_view AS
# MAGIC SELECT DISTINCT
# MAGIC     COALESCE(zd.etapa, zl.lead_status) AS nombreEtapaVenta,
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Nuevo') THEN 'Nuevo'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Asignado', 'Sin gestionar', 'Sin Gestionar', 'Contactando') THEN 'Asignado'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Contactado', 'Sin información', 'Sin Información') THEN 'Contactado'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Seguimiento', 'Valorando', 'Cita') THEN 'Seguimiento'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Interesado') THEN 'Interesado'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Pendiente de pago', 'Pendiente pago') THEN 'Pendiente de pago'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Pagado (NE)', 'Pendiente Entrevista', 'Pendiente prueba', 'Pendiente documentación', 'Pendiente Documentación') THEN 'Pagado (NE)'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Matriculado') THEN 'Matriculado'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Pagado (NEC)') THEN 'Pagado (NEC)'
# MAGIC         ELSE 'Desconocido'  -- En caso de valores no contemplados
# MAGIC     END AS nombreEtapaVentaAgrupado
# MAGIC     ,CASE 
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Nuevo%' THEN 0
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Asignado%' THEN 0
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Sin gestionar%' THEN 0
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Contactando%' THEN 0
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Contactado%' THEN 0
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Sin información%' THEN 0
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Seguimiento%' THEN 0
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Valorando%' THEN 0
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Cita%' THEN 0
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Interesado%' THEN 0
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Pendiente de pago%' THEN 0
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Pagado (NE)%' THEN 1
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Pendiente Entrevista%' THEN 1
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Pendiente prueba%' THEN 1
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Pendiente documentación%' THEN 1
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Pendiente Documentación%' THEN 1
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Matriculado%' THEN 1
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Pagado (NEC)%' THEN 1
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'NEC%' THEN 1
# MAGIC         ELSE 0 
# MAGIC     END AS esNE,
# MAGIC     CURRENT_TIMESTAMP AS ETLcreatedDate,
# MAGIC     CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM silver_lakehouse.zoholeads zl
# MAGIC FULL OUTER JOIN silver_lakehouse.zohodeals zd
# MAGIC ON zl.lead_status = zd.etapa
# MAGIC WHERE COALESCE(zd.etapa, zl.lead_status) IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     COALESCE(zd.etapa, zl.lead_status) AS nombreEtapaVenta,
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Nuevo') THEN 'Nuevo'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Asignado', 'Sin gestionar', 'Sin Gestionar', 'Contactando') THEN 'Asignado'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Contactado', 'Sin información', 'Sin Información') THEN 'Contactado'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Seguimiento', 'Valorando', 'Cita') THEN 'Seguimiento'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Interesado') THEN 'Interesado'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Pendiente de pago', 'Pendiente pago') THEN 'Pendiente de pago'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Pagado (NE)', 'Pendiente Entrevista', 'Pendiente prueba', 'Pendiente documentación', 'Pendiente Documentación') THEN 'Pagado (NE)'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Matriculado') THEN 'Matriculado'
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) IN ('Pagado (NEC)') THEN 'Pagado (NEC)'
# MAGIC         ELSE 'Desconocido'
# MAGIC     END AS nombreEtapaVentaAgrupado,
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(zd.etapa, zl.lead_status) LIKE 'Pagado (NE)%' OR
# MAGIC              COALESCE(zd.etapa, zl.lead_status) LIKE 'Pendiente Entrevista%' OR
# MAGIC              COALESCE(zd.etapa, zl.lead_status) LIKE 'Pendiente prueba%' OR
# MAGIC              COALESCE(zd.etapa, zl.lead_status) LIKE 'Pendiente documentación%' OR
# MAGIC              COALESCE(zd.etapa, zl.lead_status) LIKE 'Pendiente Documentación%' OR
# MAGIC              COALESCE(zd.etapa, zl.lead_status) LIKE 'Matriculado%' OR
# MAGIC              COALESCE(zd.etapa, zl.lead_status) LIKE 'Pagado (NEC)%' OR
# MAGIC              COALESCE(zd.etapa, zl.lead_status) LIKE 'NEC%' 
# MAGIC         THEN 1 ELSE 0 
# MAGIC     END AS esNE,
# MAGIC     CURRENT_TIMESTAMP AS ETLcreatedDate,
# MAGIC     CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM silver_lakehouse.zoholeads_38b zl
# MAGIC FULL OUTER JOIN silver_lakehouse.zohodeals_38b zd
# MAGIC   ON zl.lead_status = zd.etapa
# MAGIC WHERE COALESCE(zd.etapa, zl.lead_status) IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_etapa_venta AS target
# MAGIC USING dim_etapa_venta_view AS source
# MAGIC ON target.nombre_etapa_venta = source.nombreEtapaVenta
# MAGIC
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.nombreEtapaVentaAgrupado = source.nombreEtapaVentaAgrupado,
# MAGIC         target.esNE = source.esNE,
# MAGIC         target.ETLupdatedDate = source.ETLupdatedDate
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         nombre_etapa_venta,
# MAGIC         nombreEtapaVentaAgrupado,
# MAGIC         esNE,
# MAGIC         ETLcreatedDate,
# MAGIC         ETLupdatedDate
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.nombreEtapaVenta,
# MAGIC         source.nombreEtapaVentaAgrupado,
# MAGIC         source.esNE,
# MAGIC         source.ETLcreatedDate,
# MAGIC         source.ETLupdatedDate
# MAGIC     );
