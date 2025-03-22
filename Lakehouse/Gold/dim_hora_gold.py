# Databricks notebook source
# DBTITLE 1,Insert Into dim_hora
# MAGIC %sql
# MAGIC -- Insertar datos en la tabla dim_hora solo si no hay registros existentes
# MAGIC WITH horas AS (
# MAGIC     SELECT sequence(0, 23) AS horas
# MAGIC ),
# MAGIC minutos AS (
# MAGIC     SELECT sequence(0, 59) AS minutos
# MAGIC ),
# MAGIC combinaciones AS (
# MAGIC     SELECT explode(horas) AS hora, explode(minutos) AS minuto
# MAGIC     FROM horas, minutos
# MAGIC )
# MAGIC INSERT INTO gold_lakehouse.dim_hora
# MAGIC SELECT
# MAGIC     hora * 100 + minuto AS id_dim_hora, -- Llave única combinando hora y minuto
# MAGIC     format_string('%02d:%02d:00', hora, minuto) AS id_dim_hora_larga, -- Formato hh:mm:00
# MAGIC     hora AS hora, -- Hora (00 a 23)
# MAGIC     minuto AS minuto, -- Minuto (00 a 59)
# MAGIC     CASE 
# MAGIC         WHEN hora = 0 THEN 12
# MAGIC         WHEN hora > 12 THEN hora - 12
# MAGIC         ELSE hora
# MAGIC     END AS hora12,
# MAGIC     CASE 
# MAGIC         WHEN hora >= 12 THEN 'PM'
# MAGIC         ELSE 'AM'
# MAGIC     END AS pmam -- AM o PM
# MAGIC FROM combinaciones
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM gold_lakehouse.dim_hora
# MAGIC     WHERE dim_hora.id_dim_hora = hora * 100 + minuto
# MAGIC ); --id_dim_hora 2359
# MAGIC

# COMMAND ----------

# DBTITLE 1,Select dim_hora
# MAGIC %sql
# MAGIC SELECT * FROM gold_lakehouse.dim_hora;
