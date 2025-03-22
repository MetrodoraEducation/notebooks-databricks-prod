# Databricks notebook source
# MAGIC %md
# MAGIC DimMotivoPerdidaLlamada

# COMMAND ----------

MotivoPerdidaLlamada_df = spark.sql("select distinct ifnull(missed_call_reason,'' ) as missed_call_reason from silver_lakehouse.aircallcalls ")

# COMMAND ----------

MotivoPerdidaLlamada_df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ Asegurar que el ID -1 solo exista una vez
# MAGIC MERGE INTO gold_lakehouse.dim_motivo_perdida_llamada AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS motivo_perdida_llamada, 'n/a' AS tipo_perdida
# MAGIC ) AS source
# MAGIC ON target.id_dim_motivo_perdida_llamada = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (motivo_perdida_llamada, tipo_perdida)
# MAGIC     VALUES ('n/a', 'n/a');
# MAGIC
# MAGIC -- 2️⃣ Insertar nuevos valores de manera controlada
# MAGIC MERGE INTO gold_lakehouse.dim_motivo_perdida_llamada AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT 
# MAGIC         missed_call_reason AS motivo_perdida_llamada,
# MAGIC         CASE 
# MAGIC             WHEN missed_call_reason IN ('abandoned_in_ivr', 'abandoned_in_classic', 'short_abandoned') THEN 'Abandono'
# MAGIC             WHEN missed_call_reason IN ('no_available_agent', 'agents_did_not_answer') THEN 'Comercial'
# MAGIC             ELSE 'n/a'
# MAGIC         END AS tipo_perdida
# MAGIC     FROM source_view
# MAGIC ) AS source
# MAGIC ON target.motivo_perdida_llamada = source.motivo_perdida_llamada
# MAGIC
# MAGIC -- Si el motivo ya existe, no se hace nada.
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (motivo_perdida_llamada, tipo_perdida)
# MAGIC     VALUES (source.motivo_perdida_llamada, source.tipo_perdida);
# MAGIC
# MAGIC -- 3️⃣ Asegurar que solo haya un único ID `-1`
# MAGIC DELETE FROM gold_lakehouse.dim_motivo_perdida_llamada
# MAGIC WHERE motivo_perdida_llamada = 'n/a' AND id_dim_motivo_perdida_llamada <> -1;
# MAGIC
