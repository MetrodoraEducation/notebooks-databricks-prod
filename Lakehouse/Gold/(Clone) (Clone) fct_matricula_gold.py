# Databricks notebook source
# MAGIC %md
# MAGIC ###FCT_MATRICULA

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from gold_lakehouse.fct_matricula
# MAGIC select * from gold_lakehouse.fct_matricula order by id_matricula desc
