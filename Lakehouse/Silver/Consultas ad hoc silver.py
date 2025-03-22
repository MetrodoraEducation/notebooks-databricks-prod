# Databricks notebook source
# MAGIC %sql
# MAGIC --drop TABLE silver_lakehouse.aircallcalls
# MAGIC --select distinct missed_call_reason from  silver_lakehouse.aircallcalls --limit 10 --30158
# MAGIC select cast(cast(max(started_at) as date) as string) as maxdate from silver_lakehouse.aircallcalls
# MAGIC --select * from silver_lakehouse.aircallcalls limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop TABLE silver_lakehouse.aircallcalls
# MAGIC select distinct country_code_a2 from  silver_lakehouse.aircallcalls --limit 10 --30158
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop TABLE silver_lakehouse.aircallcalls
# MAGIC --select distinct calls_user_name from  silver_lakehouse.aircallcalls --limit 10 --30158
# MAGIC select * from silver_lakehouse.aircallcalls limit 100
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop TABLE silver_lakehouse.aircallcalls
# MAGIC select* from  silver_lakehouse.aircallcalls limit 10 --30158
# MAGIC --update silver_lakehouse.aircallcalls set user_name = 'n/a' where user_name =''

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop TABLE silver_lakehouse.clientifydeals
# MAGIC --select count(*) from silver_lakehouse.clientifydeals
# MAGIC select distinct custom_fields_pais from silver_lakehouse.clientifydeals

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop TABLE silver_lakehouse.odoolead 
# MAGIC --select distinct x_codmodalidad from silver_lakehouse.odoolead limit 100
# MAGIC --select distinct x_modalidad_value from silver_lakehouse.odoolead limit 100
# MAGIC --select distinct country_value from silver_lakehouse.odoolead
# MAGIC select * from silver_lakehouse.odoolead

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from silver_lakehouse.sales where sistema_origen='Odoo'
# MAGIC --select * from silver_lakehouse.sales --where sistema_origen='Clientify'
# MAGIC --delete from silver_lakehouse.sales --where sistema_origen='Odoo'
# MAGIC select max(fec_creacion) from silver_lakehouse.sales where sistema_origen='Odoo'
# MAGIC --select distinct pais from silver_lakehouse.sales where sistema_origen='Odoo'
# MAGIC --select distinct origen_campania from silver_lakehouse.sales --where sistema_origen='Odoo'

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop TABLE silver_lakehouse.dim_pais
# MAGIC select name from silver_lakehouse.dim_pais

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct origen_campania from silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct sede from silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct modalidad, sistema_origen from silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct titulacion from silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC --delete from silver_lakehouse.dim_pais

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct x_ga_campaign from silver_lakehouse.odoolead

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct custom_fields_utm_campaign_name from silver_lakehouse.clientifydeals

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct status_desc from silver_lakehouse.clientifydeals

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct modalidad, sistema_origen from silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct titulacion, sistema_origen from silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct sede, sistema_origen from silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct localidad from silver_lakehouse.sales
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct motivo_cierre from silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_lakehouse.sales limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_lakehouse.budget limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(write_date) from silver_lakehouse.odoolead limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from gold_lakehouse.dim_comercial where id_dim_comercial>75-- order by id_dim_comercial--limit 1
# MAGIC delete from gold_lakehouse.dim_comercial where id_dim_comercial>75
