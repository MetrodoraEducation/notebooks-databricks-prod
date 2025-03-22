# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_COMERCIAL**

# COMMAND ----------

# MAGIC %md
# MAGIC  Explicación Detallada del Código
# MAGIC ✅ Vista Temporal (dim_comercial_temp_view):
# MAGIC
# MAGIC Extrae los datos desde zohousers y los prepara para la actualización.
# MAGIC Establece fechaDesde = current_date(), mientras que fechaHasta queda NULL.
# MAGIC ✅ MERGE con dim_comercial:
# MAGIC
# MAGIC Si el equipoComercial cambia, se cierra el registro anterior (fechaHasta = current_date()) y se inserta un nuevo registro con fechaDesde = current_date().
# MAGIC Si el codComercial no existe, se inserta directamente.
# MAGIC ✅ Cálculo de esActivo:
# MAGIC
# MAGIC Un comercial es activo (1) si no tiene fechaHasta o si fechaDesde ≤ current_date < fechaHasta.
# MAGIC Es inactivo (0) si su fechaHasta ya ha pasado.

# COMMAND ----------

# MAGIC %md
# MAGIC **Gestion Zoho**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dim_comercial_temp_view AS
# MAGIC SELECT DISTINCT
# MAGIC     z.id AS cod_Comercial,
# MAGIC     z.full_name AS nombre_Comercial,
# MAGIC     z.role_name AS equipo_Comercial,
# MAGIC     current_date() AS fecha_Desde,
# MAGIC     NULL AS fecha_Hasta
# MAGIC FROM silver_lakehouse.zohousers z;
# MAGIC
# MAGIC select * from dim_comercial_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ Asegurar que el registro `-1` siempre existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_comercial AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS cod_comercial, 'n/a' AS nombre_comercial, 'n/a' AS equipo_comercial, NULL AS activo, NULL AS fecha_desde, NULL AS fecha_hasta
# MAGIC ) AS source
# MAGIC ON target.id_dim_comercial = -1  -- Solo se compara con `-1`
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (cod_comercial, nombre_comercial, equipo_comercial, activo, fecha_desde, fecha_hasta)
# MAGIC     VALUES (source.cod_comercial, source.nombre_comercial, source.equipo_comercial, source.activo, source.fecha_desde, source.fecha_hasta);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Cerrar el registro anterior si cambia el equipo comercial
# MAGIC MERGE INTO gold_lakehouse.dim_comercial AS target
# MAGIC USING dim_comercial_temp_view AS source
# MAGIC ON COALESCE(target.cod_Comercial, '') = COALESCE(source.cod_Comercial, '')
# MAGIC AND target.fecha_Hasta IS NULL  -- Solo cerramos registros abiertos
# MAGIC AND TRIM(UPPER(target.equipo_Comercial)) <> TRIM(UPPER(source.equipo_Comercial))
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET 
# MAGIC         target.fecha_Hasta = CURRENT_DATE(),
# MAGIC         target.activo = 0;  -- Marcamos como inactivo
# MAGIC
# MAGIC -- 2️⃣ 🔹 Insertar nuevos registros para comerciales cerrados o inexistentes
# MAGIC INSERT INTO gold_lakehouse.dim_comercial (cod_Comercial, nombre_Comercial, equipo_Comercial, fecha_Desde, fecha_Hasta, activo)
# MAGIC SELECT 
# MAGIC     source.cod_Comercial,
# MAGIC     source.nombre_Comercial,
# MAGIC     source.equipo_Comercial,
# MAGIC     CURRENT_DATE(), -- La nueva fecha desde es la de hoy
# MAGIC     NULL, -- Se deja abierto hasta que vuelva a cambiar
# MAGIC     1  -- Activo
# MAGIC FROM dim_comercial_temp_view source
# MAGIC LEFT JOIN gold_lakehouse.dim_comercial target
# MAGIC ON COALESCE(source.cod_Comercial, '') = COALESCE(target.cod_Comercial, '')
# MAGIC AND target.fecha_Hasta IS NULL -- Nos aseguramos de comparar solo los registros abiertos
# MAGIC WHERE target.cod_Comercial IS NULL OR target.fecha_Hasta IS NOT NULL; -- 🔹 Insertar si no existe o si el último registro está cerrado

# COMMAND ----------

# MAGIC %md
# MAGIC **Gestion Calls**

# COMMAND ----------

#%sql
#-- 1️⃣ Asegurar que el ID -1 solo exista una vez con valores 'n/a'
#MERGE INTO gold_lakehouse.dim_comercial AS target
#USING (
#    SELECT 'n/a' AS nombre_comercial, 'n/a' AS equipo_comercial, 'n/a' AS cod_comercial, 
#           CAST(NULL AS INTEGER) AS activo, DATE('1900-01-01') AS fecha_desde, DATE('1900-01-01') AS fecha_hasta
#) AS source
#ON target.id_dim_comercial = -1
#WHEN NOT MATCHED THEN 
#    INSERT (nombre_comercial, equipo_comercial, cod_comercial, activo, fecha_desde, fecha_hasta)
#    VALUES ('n/a', 'n/a', 'n/a', NULL, DATE('1900-01-01'), DATE('1900-01-01'));
#
#-- 2️⃣ Realizar el MERGE de comerciales sin afectar `id_dim_comercial = -1`
#MERGE INTO gold_lakehouse.dim_comercial AS target
#USING (
#    SELECT DISTINCT 
#        nombre_comercial, 
#        equipo_comercial, 
#        cod_comercial, 
#        0 as activo, 
#        fecha_desde,
#        fecha_hasta
#    FROM dim_comercial_temp_view
#) AS source
#ON UPPER(target.nombre_comercial) = UPPER(source.nombre_comercial) 
#AND target.id_dim_comercial != -1  -- 🔹 Evita modificar el registro `-1`
#
#-- 🔹 Si el registro ya existe, actualizar solo si cambian valores clave
#WHEN MATCHED THEN 
#    UPDATE SET 
#        target.equipo_comercial = source.equipo_comercial,
#        target.cod_comercial = source.cod_comercial
#
#-- 🔹 Si el registro no existe, se inserta (sin tocar `id_dim_comercial` que se autogenera)
#WHEN NOT MATCHED THEN 
#    INSERT (nombre_comercial, equipo_comercial, cod_comercial, activo, fecha_desde, fecha_hasta)
#    VALUES (source.nombre_comercial, source.equipo_comercial, source.cod_comercial, source.activo, source.fecha_desde, source.fecha_hasta);

# COMMAND ----------

#%sql
#CREATE OR REPLACE TEMPORARY VIEW comercial_sales_view
#    AS SELECT DISTINCT 
#    propietario_lead AS nombre_comercial,
#    institucion AS equipo_comercial,
#    '' AS cod_comercial,  -- Si no tienes esta columna en la fuente, se asigna un valor vacío
#    TRUE AS activo,       -- Si no tienes la columna, puedes definirla como TRUE o algún valor por defecto
#    CURRENT_DATE() AS fecha_desde,  -- Si no tienes la columna, usa la fecha actual como predeterminado
#    NULL AS fecha_hasta
#FROM silver_lakehouse.sales
#WHERE propietario_lead <> 'n/a';
#
#SELECT * FROM comercial_sales_view;

# COMMAND ----------

#%sql
#CREATE OR REPLACE TEMPORARY VIEW comercial_calls_view
#    AS SELECT distinct user_name from silver_lakehouse.aircallcalls where user_name<>'n/a';
#
#SELECT * FROM comercial_calls_view;

# COMMAND ----------

#%sql
#MERGE INTO gold_lakehouse.dim_comercial AS target
#USING comercial_calls_view AS source
#ON target.nombre_comercial = source.user_name
#
#WHEN NOT MATCHED THEN 
#    INSERT (cod_Comercial, nombre_comercial, equipo_comercial, activo, fecha_desde)
#    VALUES ('', source.user_name, 'n/a', 1, current_date());
