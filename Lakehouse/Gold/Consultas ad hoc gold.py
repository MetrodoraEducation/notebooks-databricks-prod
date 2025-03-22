# Databricks notebook source
# MAGIC %sql
# MAGIC --delete from gold_lakehouse.dim_pais
# MAGIC select *   from gold_lakehouse.dim_pais

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_comercial

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from gold_lakehouse.dim_comercial where id_dim_comercial=1
# MAGIC --update gold_lakehouse.dim_comercial set nombre_comercial='n/a', equipo_comercial='n/a' where id_dim_comercial=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.fct_llamada order by id_dim_hora asc--limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_motivo_perdida_llamada limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_lakehouse.dim_hora 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_lakehouse.dim_fecha order by   limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(started_at) from silver_lakehouse.aircallcalls limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from gold_lakehouse.fct_llamada 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_comercial limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC --Select cod_venta, count(cod_venta) from  gold_lakehouse.fct_venta group by cod_venta having(count(cod_venta) > 1) --limit 100
# MAGIC --delete from gold_lakehouse.fct_venta
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(cod_venta) from  silver_lakehouse.sales --group by cod_venta having(count(cod_venta) > 1) --limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC Select estudio, count(estudio) from  silver_lakehouse.mapeo_estudio group by estudio having(count(estudio) > 1) --limit 100
# MAGIC --delete from silver_lakehouse.mapeo_estudio
