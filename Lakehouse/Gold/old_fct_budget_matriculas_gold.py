# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW budget_matriculas_view AS
# MAGIC WITH producto_fechas AS (
# MAGIC     SELECT 
# MAGIC         m.id_dim_producto,
# MAGIC         p.cod_producto,
# MAGIC         MIN(m.fec_matricula) AS fecha_inicio
# MAGIC     FROM 
# MAGIC         gold_lakehouse.fct_matricula m
# MAGIC     JOIN 
# MAGIC         gold_lakehouse.dim_producto p 
# MAGIC         ON m.id_dim_producto = p.id_Dim_Producto
# MAGIC     WHERE 
# MAGIC         m.fec_matricula IS NOT NULL
# MAGIC     GROUP BY 
# MAGIC         m.id_dim_producto, p.cod_producto
# MAGIC ),
# MAGIC calendario_expandido AS (
# MAGIC     SELECT 
# MAGIC         pf.id_dim_producto,
# MAGIC         pf.cod_producto,
# MAGIC         df.id_dim_fecha AS fecha
# MAGIC     FROM 
# MAGIC         producto_fechas pf
# MAGIC     JOIN gold_lakehouse.dim_fecha df
# MAGIC         ON df.id_dim_fecha BETWEEN pf.fecha_inicio AND to_date(concat(year(current_date), '-12-31'))
# MAGIC ),
# MAGIC matriculas_diarias AS (
# MAGIC     SELECT
# MAGIC         m.id_dim_producto,
# MAGIC         m.fec_matricula AS fecha,
# MAGIC         COUNT(*) AS numMatriculas,
# MAGIC         SUM(m.importe_matricula) AS importeVentaNeta
# MAGIC     FROM 
# MAGIC         gold_lakehouse.fct_matricula m
# MAGIC     WHERE 
# MAGIC         m.fec_matricula IS NOT NULL
# MAGIC     GROUP BY 
# MAGIC         m.id_dim_producto, m.fec_matricula
# MAGIC )
# MAGIC SELECT 
# MAGIC     c.fecha,
# MAGIC     'Budget' AS escenario,
# MAGIC     c.cod_producto AS producto,
# MAGIC     COALESCE(m.numMatriculas, 0) AS numMatriculas,
# MAGIC     COALESCE(m.importeVentaNeta, 0.00) AS importeVentaNeta,
# MAGIC     0 AS numLeadsNetos,
# MAGIC     0 AS numLeadsBrutos,
# MAGIC     0 AS importeCaptacion
# MAGIC FROM 
# MAGIC     calendario_expandido c
# MAGIC LEFT JOIN 
# MAGIC     matriculas_diarias m
# MAGIC     ON c.fecha = m.fecha AND c.id_dim_producto = m.id_dim_producto
# MAGIC ORDER BY 
# MAGIC     c.cod_producto, c.fecha;

# COMMAND ----------

#%sql select * from gold_lakehouse.dim_producto

# COMMAND ----------

#%sql select * from gold_lakehouse.fct_matricula

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fct_budget_view
# MAGIC     AS SELECT 
# MAGIC       a.fecha  as fec_budget,	
# MAGIC       c.id_dim_escenario_budget,	
# MAGIC       b.id_dim_titulacion_budget,	
# MAGIC       a.centro,
# MAGIC       a.sede,
# MAGIC       a.modalidad,	
# MAGIC       a.num_leads_netos,	
# MAGIC       a.num_leads_brutos,	
# MAGIC       a.new_enrollment as num_matriculas,
# MAGIC       a.importe_venta_neta,	
# MAGIC       a.importe_venta_bruta,	
# MAGIC       a.importe_captacion,
# MAGIC       a.processdate as fec_procesamiento
# MAGIC     FROM silver_budget_view a 
# MAGIC     LEFT JOIN gold_lakehouse.dim_titulacion_budget b ON a.titulacion = b.titulacion
# MAGIC     LEFT JOIN gold_lakehouse.dim_escenario_budget c ON a.escenario = c.escenario
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE WITH SCHEMA EVOLUTION 
# MAGIC INTO gold_lakehouse.fct_budget
# MAGIC USING fct_budget_view 
# MAGIC ON gold_lakehouse.fct_budget.fec_budget = fct_budget_view.fec_budget 
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
