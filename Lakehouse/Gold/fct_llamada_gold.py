# Databricks notebook source
aircall_df = spark.sql("select * from silver_lakehouse.aircallcalls WHERE processdate > (SELECT IFNULL(max(processdate),'1900-01-01') FROM gold_lakehouse.fct_llamada)")
dim_fecha_df = spark.sql("select * from gold_lakehouse.dim_fecha")
dim_hora_df = spark.sql("select * from gold_lakehouse.dim_hora")
dim_pais_df = spark.sql("select * from gold_lakehouse.dim_pais")
dim_comercial_df = spark.sql("select * from gold_lakehouse.dim_comercial")
dim_motivo_perdida_llamada_df = spark.sql("select * from gold_lakehouse.dim_motivo_perdida_llamada")


# COMMAND ----------

aircall_df.createOrReplaceTempView("aircall_view")
dim_fecha_df.createOrReplaceTempView("dim_fecha_view")
dim_hora_df.createOrReplaceTempView("dim_hora_view")
dim_pais_df.createOrReplaceTempView("dim_pais_view")
dim_comercial_df.createOrReplaceTempView("dim_comercial_view")
dim_motivo_perdida_llamada_df.createOrReplaceTempView("dim_motivo_perdida_llamada_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear vista temporal para transformar los datos de la tabla de aircall_raw
# MAGIC CREATE OR REPLACE TEMP VIEW fct_llamada_transformed AS
# MAGIC SELECT
# MAGIC     a.id AS cod_llamada
# MAGIC     , to_date(a.started_at) as fec_llamada
# MAGIC     --,b.id_dim_fecha as fec_llamada
# MAGIC     ,c.id_dim_hora_larga as hora_llamada
# MAGIC     ,a.direction AS direccion_llamada
# MAGIC     ,a.duration AS duration
# MAGIC     ,a.raw_digits AS numero_telefono
# MAGIC     ,d.id AS id_dim_pais
# MAGIC     ,e.id_dim_comercial AS id_dim_comercial
# MAGIC     ,f.id_dim_motivo_perdida_llamada AS id_dim_motivo_perdida_llamada
# MAGIC     ,a.processdate as processdate --timestamp simplemente te copias processdate de la capa silver --processdate aqui lo vamos a llamar fec_procesamiento a.processdate
# MAGIC FROM aircall_view a
# MAGIC --LEFT JOIN dim_fecha_view b ON DATE(a.started_at) = b.id_dim_fecha
# MAGIC LEFT JOIN dim_hora_view c ON HOUR(a.started_at) = c.hora AND MINUTE(a.started_at) = c.minuto
# MAGIC LEFT JOIN dim_pais_view d ON a.country_code_a2 = d.iso2
# MAGIC LEFT JOIN dim_comercial_view e ON a.user_name = e.nombre_comercial
# MAGIC LEFT JOIN dim_motivo_perdida_llamada_view f ON a.missed_call_reason = f.motivo_perdida_llamada;
# MAGIC     --WHERE a.processdate > (SELECT IFNULL(max(processdate),'1900-01-01') FROM gold_lakehouse.fct_llamada);
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Validate that there are no duplicates fact_llamada_transformed
# MAGIC %sql
# MAGIC --SELECT cod_llamada, COUNT(*) FROM fct_llamada_transformed GROUP BY cod_llamada HAVING COUNT(*) > 1;

# COMMAND ----------

# DBTITLE 1,Merge into gold_lakehouse.fct_llamada
# MAGIC %sql
# MAGIC WITH deduplicated_source AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY cod_llamada ORDER BY fec_llamada DESC) AS row_num
# MAGIC   FROM
# MAGIC     fct_llamada_transformed
# MAGIC )
# MAGIC MERGE INTO gold_lakehouse.fct_llamada AS target
# MAGIC USING (
# MAGIC   SELECT * FROM deduplicated_source WHERE row_num = 1
# MAGIC ) AS source
# MAGIC ON target.cod_llamada = source.cod_llamada
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET
# MAGIC     target.id_dim_tiempo = source.fec_llamada, 
# MAGIC     target.id_dim_hora = source.hora_llamada, 
# MAGIC     target.direccion_llamada = source.direccion_llamada,
# MAGIC     target.duration = source.duration,
# MAGIC     target.numero_telefono = source.numero_telefono,
# MAGIC     target.id_dim_pais = source.id_dim_pais,
# MAGIC     target.id_dim_comercial = source.id_dim_comercial,
# MAGIC     target.id_dim_motivo_perdida_llamada = source.id_dim_motivo_perdida_llamada,
# MAGIC     target.processdate = current_timestamp()  
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT (
# MAGIC     cod_llamada,
# MAGIC     id_dim_tiempo,                                          
# MAGIC     id_dim_hora,                                            
# MAGIC     direccion_llamada,                                      
# MAGIC     duration,                                               
# MAGIC     numero_telefono,                                         
# MAGIC     id_dim_pais,                                              
# MAGIC     id_dim_comercial,                                         
# MAGIC     id_dim_motivo_perdida_llamada,                             
# MAGIC     processdate  
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.cod_llamada,
# MAGIC     source.fec_llamada, 
# MAGIC     source.hora_llamada,
# MAGIC     source.direccion_llamada,
# MAGIC     source.duration,
# MAGIC     source.numero_telefono,
# MAGIC     source.id_dim_pais,
# MAGIC     source.id_dim_comercial,
# MAGIC     source.id_dim_motivo_perdida_llamada,
# MAGIC     current_timestamp()  
# MAGIC   );
