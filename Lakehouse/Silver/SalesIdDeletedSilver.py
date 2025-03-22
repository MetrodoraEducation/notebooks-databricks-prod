# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW clientifyidfordelete_view
# MAGIC    AS SELECT * from silver_lakehouse.clientifydealsidfordelete
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW clientifytot_view 
# MAGIC   as select * FROM silver_lakehouse.sales where sistema_origen='Clientify'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW clientifyiddeleted_view 
# MAGIC   as select cod_venta FROM clientifytot_view where cod_venta not in (select id from clientifyidfordelete_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE --WITH SCHEMA EVOLUTION 
# MAGIC INTO silver_lakehouse.sales as a
# MAGIC USING clientifyiddeleted_view as b 
# MAGIC ON a.cod_venta = b.cod_venta and a.sistema_origen = 'Clientify'
# MAGIC WHEN MATCHED THEN UPDATE SET a.activo = 0
# MAGIC
