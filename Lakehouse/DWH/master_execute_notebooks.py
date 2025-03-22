# Databricks notebook source
# DBTITLE 1,dim_campania
# MAGIC %run "../DWH/dim_campania"

# COMMAND ----------

# DBTITLE 1,dim_comercial
# MAGIC %run "../DWH/dim_comercial"

# COMMAND ----------

# DBTITLE 1,dim_estado_venta
# MAGIC %run "../DWH/dim_estado_venta"

# COMMAND ----------

# DBTITLE 1,dim_etapa_venta
# MAGIC %run "../DWH/dim_etapa_venta"

# COMMAND ----------

# DBTITLE 1,dim_institucion
# MAGIC %run "../DWH/dim_institucion"

# COMMAND ----------

# DBTITLE 1,dim_localidad
# MAGIC %run "../DWH/dim_localidad"

# COMMAND ----------

# DBTITLE 1,dim_modalidad
# MAGIC %run "../DWH/dim_modalidad"

# COMMAND ----------

# DBTITLE 1,dim_motivo_cierre
# MAGIC %run "../DWH/dim_motivo_cierre"

# COMMAND ----------

# DBTITLE 1,dim_motivo_perdida_llamada
# MAGIC %run "../DWH/dim_motivo_perdida_llamada"

# COMMAND ----------

# DBTITLE 1,dim_origen_campania
# MAGIC %run "../DWH/dim_origen_campania"

# COMMAND ----------

# DBTITLE 1,dim_sede
# MAGIC %run "../DWH/dim_sede"

# COMMAND ----------

# DBTITLE 1,dim_tipo_formacion
# MAGIC %run "../DWH/dim_tipo_formacion"

# COMMAND ----------

# DBTITLE 1,dim_tipo_negocio
# MAGIC %run "../DWH/dim_tipo_negocio"

# COMMAND ----------

# DBTITLE 1,fct_llamada
# MAGIC %run "../DWH/fct_llamada"

# COMMAND ----------

# DBTITLE 1,fct_venta
# MAGIC %run "../DWH/fct_venta"

# COMMAND ----------

# DBTITLE 1,mapeo_origen_campania
# MAGIC %run "../DWH/mapeo_origen_campania"

# COMMAND ----------

# DBTITLE 1,dim_producto
# MAGIC %run "../DWH/dim_producto"

# COMMAND ----------

# DBTITLE 1,dim_programa
# MAGIC %run "../DWH/dim_programa"

# COMMAND ----------

# DBTITLE 1,dim_vertical
# MAGIC %run "../DWH/dim_vertical"

# COMMAND ----------

# DBTITLE 1,dim_entidad_legal
# MAGIC %run "../DWH/dim_entidad_legal"

# COMMAND ----------

# DBTITLE 1,dim_motivo_perdida
# MAGIC %run "../DWH/dim_motivo_perdida"

# COMMAND ----------

# DBTITLE 1,dim_especialidad
# MAGIC %run "../DWH/dim_especialidad"

# COMMAND ----------

# DBTITLE 1,dim_utm_campaign
# MAGIC %run "../DWH/dim_utm_campaign"

# COMMAND ----------

# DBTITLE 1,dim_utm_adset
# MAGIC %run "../DWH/dim_utm_adset"

# COMMAND ----------

# DBTITLE 1,dim_utm_source
# MAGIC %run "../DWH/dim_utm_source"

# COMMAND ----------

# DBTITLE 1,dim_nacionalidad
# MAGIC %run "../DWH/dim_nacionalidad"

# COMMAND ----------

# DBTITLE 1,fctventa_zoho
# MAGIC %run "../DWH/fctventa_zoho"

# COMMAND ----------

# DBTITLE 1,dim_estado_matricula
# MAGIC %run "../DWH/dim_estado_matricula"

# COMMAND ----------

# DBTITLE 1,dim_estudiante
# MAGIC %run "../DWH/dim_estudiante"

# COMMAND ----------

# DBTITLE 1,fct_matricula
# MAGIC %run "../DWH/fct_matricula"

# COMMAND ----------

# MAGIC %run "../DWH/dim_pais"

# COMMAND ----------

# MAGIC %run "../DWH/dim_estudio"

# COMMAND ----------

# MAGIC %run "../DWH/dim_fecha" 

# COMMAND ----------

# MAGIC %run "../DWH/dim_hora" 
