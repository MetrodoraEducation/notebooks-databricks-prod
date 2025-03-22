# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silver_budget_view
# MAGIC     AS SELECT * FROM silver_lakehouse.budget

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
# MAGIC
# MAGIC     
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
