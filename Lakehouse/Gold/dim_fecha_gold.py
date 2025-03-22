# Databricks notebook source
# DBTITLE 1,Insert Into dim_fecha
# MAGIC %sql
# MAGIC WITH date_series AS (
# MAGIC     -- Generar fechas desde 2023-08-01 hasta la fecha actual
# MAGIC     SELECT sequence(to_date('2023-08-01'), current_date(), interval 1 day) AS dates
# MAGIC ),
# MAGIC new_data AS (
# MAGIC     SELECT 
# MAGIC         date AS id_dim_fecha, -- Fecha única como clave primaria
# MAGIC         cast(date AS TIMESTAMP) AS fecha_larga, -- Usar el mismo formato de DATE
# MAGIC         date_format(date, 'EEEE') AS dia_semana, -- Día completo de la semana (e.g., Lunes)
# MAGIC         date_format(date, 'E') AS dia_semana_corto, -- Día corto (e.g., Lun)
# MAGIC         dayofweek(date) AS dia_semana_numero, -- Número del día de la semana (1=Domingo, 7=Sábado)
# MAGIC         month(date) AS mes_numero, -- Número del mes
# MAGIC         date_format(date, 'MMM') AS mes_corto, -- Mes corto (e.g., Ene)
# MAGIC         date_format(date, 'MMMM') AS mes_largo, -- Nombre completo del mes (e.g., Enero)
# MAGIC         dayofyear(date) AS dia_anio, -- Día del año
# MAGIC         weekofyear(date) AS semana_anio, -- Número de la semana en el año
# MAGIC         day(last_day(date)) AS numero_dias_mes, -- Total de días en el mes
# MAGIC         day(date) AS dia_mes, -- 📌 Día del mes agregado
# MAGIC         trunc(date, 'month') AS primer_dia_mes, -- Primer día del mes
# MAGIC         last_day(date) AS ultimo_dia_mes, -- Último día del mes
# MAGIC         year(date) AS anio_numero, -- Año (e.g., 2023)
# MAGIC         CASE 
# MAGIC             WHEN month(date) IN (9, 10, 11) THEN 1 -- Q1: Sep-Nov
# MAGIC             WHEN month(date) IN (12, 1, 2) THEN 2 -- Q2: Dic-Feb
# MAGIC             WHEN month(date) IN (3, 4, 5) THEN 3 -- Q3: Mar-May
# MAGIC             ELSE 4 -- Q4: Jun-Ago
# MAGIC         END AS trimestre_numero, -- Número del trimestre fiscal (1, 2, 3, 4)
# MAGIC         CASE 
# MAGIC             WHEN month(date) IN (9, 10, 11) THEN 'Q1' -- Q1: Sep-Nov
# MAGIC             WHEN month(date) IN (12, 1, 2) THEN 'Q2' -- Q2: Dic-Feb
# MAGIC             WHEN month(date) IN (3, 4, 5) THEN 'Q3' -- Q3: Mar-May
# MAGIC             ELSE 'Q4' -- Q4: Jun-Ago
# MAGIC         END AS trimestre_nombre, -- Nombre del trimestre fiscal (Q1, Q2, Q3, Q4)
# MAGIC         (month(date) + CASE WHEN month(date) >= 9 THEN -8 ELSE 4 END) AS mes_fiscal_numero -- Mes fiscal numero
# MAGIC         ,CASE 
# MAGIC                 WHEN month(date) >= 9 THEN year(date)  -- Septiembre a diciembre: el año fiscal es el mismo
# MAGIC                 ELSE year(date) - 1  -- Enero a agosto: el año fiscal es el anterior
# MAGIC          END AS anio_fiscal_numero -- Año fiscal
# MAGIC         ,CONCAT(
# MAGIC                 CASE 
# MAGIC                     WHEN month(date) >= 9 THEN year(date)  
# MAGIC                     ELSE year(date) - 1
# MAGIC                 END, 
# MAGIC                 '/', 
# MAGIC                 CASE 
# MAGIC                     WHEN month(date) >= 9 THEN year(date) + 1  
# MAGIC                     ELSE year(date)
# MAGIC                 END
# MAGIC         ) AS curso_academico -- Curso academico
# MAGIC         ,CASE WHEN dayofweek(date) IN (1, 7) THEN FALSE ELSE TRUE END AS es_laborable, -- TRUE si es laborable, FALSE si no
# MAGIC         CASE WHEN dayofweek(date) IN (1, 7) THEN TRUE ELSE FALSE END AS es_finde_semana -- TRUE si es fin de semana
# MAGIC     FROM date_series
# MAGIC     LATERAL VIEW explode(dates) exploded AS date
# MAGIC )
# MAGIC
# MAGIC -- Usar MERGE INTO para insertar nuevos registros y evitar duplicados
# MAGIC MERGE INTO gold_lakehouse.dim_fecha AS target
# MAGIC USING new_data AS source
# MAGIC ON target.id_dim_fecha = source.id_dim_fecha -- Condición para evitar duplicados
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     id_dim_fecha,
# MAGIC     fecha_larga,
# MAGIC     dia_semana,
# MAGIC     dia_semana_corto,
# MAGIC     dia_semana_numero,
# MAGIC     mes_numero,
# MAGIC     mes_corto,
# MAGIC     mes_largo,
# MAGIC     dia_anio,
# MAGIC     semana_anio,
# MAGIC     numero_dias_mes,
# MAGIC     dia_mes, -- 📌 Se agrega a la inserción
# MAGIC     primer_dia_mes,
# MAGIC     ultimo_dia_mes,
# MAGIC     anio_numero,
# MAGIC     trimestre_numero,
# MAGIC     trimestre_nombre,
# MAGIC     mes_fiscal_numero,
# MAGIC     anio_fiscal_numero,
# MAGIC     curso_academico,
# MAGIC     es_laborable,
# MAGIC     es_finde_semana
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.id_dim_fecha,
# MAGIC     source.fecha_larga,
# MAGIC     source.dia_semana,
# MAGIC     source.dia_semana_corto,
# MAGIC     source.dia_semana_numero,
# MAGIC     source.mes_numero,
# MAGIC     source.mes_corto,
# MAGIC     source.mes_largo,
# MAGIC     source.dia_anio,
# MAGIC     source.semana_anio,
# MAGIC     source.numero_dias_mes,
# MAGIC     source.dia_mes, -- 📌 Se inserta en la tabla
# MAGIC     source.primer_dia_mes,
# MAGIC     source.ultimo_dia_mes,
# MAGIC     source.anio_numero,
# MAGIC     source.trimestre_numero,
# MAGIC     source.trimestre_nombre,
# MAGIC     source.mes_fiscal_numero,
# MAGIC     source.anio_fiscal_numero,
# MAGIC     source.curso_academico,
# MAGIC     source.es_laborable,
# MAGIC     source.es_finde_semana
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,Select dim_fecha
# MAGIC %sql
# MAGIC SELECT * FROM gold_lakehouse.dim_fecha;

# COMMAND ----------

# DBTITLE 1,!NOT USE! Insert Into new days dim_fecha
#%sql
#-- Incremental: Insertar solo los días nuevos en la tabla
#WITH last_date AS (
#    -- Obtener la última fecha registrada en la tabla
#    SELECT COALESCE(MAX(id_dim_fecha), to_date('2023-08-01')) AS start_date
#    FROM gold_lakehouse.dim_fecha
#),
#date_series AS (
#    SELECT sequence(
#        DATE_ADD(start_date, 1), -- Día siguiente al último registro
#        current_date(),          -- Hasta el día actual
#        interval 1 day
#    ) AS dates
#    FROM last_date
#    WHERE start_date < current_date() -- Solo generar si hay días faltantes
#)
#-- Expandir fechas generadas en filas y calcular las columnas
#INSERT INTO gold_lakehouse.dim_fecha
#SELECT 
#    date AS id_dim_fecha,
#    date_format(date, 'd \'de\' MMMM \'de\' yyyy') AS fecha_larga,
#    date_format(date, 'EEEE') AS dia_semana,
#    date_format(date, 'E') AS dia_semana_corto,
#    dayofweek(date) AS dia_semana_numero,
#    month(date) AS mes_numero,
#    date_format(date, 'MMM') AS mes_corto,
#    date_format(date, 'MMMM') AS mes_largo,
#    dayofyear(date) AS dia_anio,
#    weekofyear(date) AS semana_anio,
#    day(last_day(date)) AS numero_dias_mes,
#    trunc(date, 'month') AS primer_dia_mes,
#    last_day(date) AS ultimo_dia_mes,
#    year(date) AS anio_numero,
#    ceil(month(date) / 3) AS trimestre_numero,
#    concat('Q', ceil(month(date) / 3)) AS trimestre_nombre,
#    (month(date) + CASE WHEN month(date) >= 8 THEN -7 ELSE 5 END) AS mes_fiscal_numero,
#    (year(date) + CASE WHEN month(date) >= 8 THEN 1 ELSE 0 END) AS anio_fiscal_numero,
#    concat(year(date), '/', year(date) + 1) AS curso_academico,
#    CASE WHEN dayofweek(date) IN (1, 7) THEN 0 ELSE 1 END AS es_labrable,
#    CASE WHEN dayofweek(date) IN (1, 7) THEN 1 ELSE 0 END AS es_finde_semana
#FROM date_series
#LATERAL VIEW explode(dates) exploded AS date;
