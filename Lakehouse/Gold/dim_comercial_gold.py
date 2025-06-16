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
# MAGIC     z.email AS email,
# MAGIC     current_date() AS fecha_Desde,
# MAGIC     NULL AS fecha_Hasta
# MAGIC FROM silver_lakehouse.zohousers z
# MAGIC UNION ALL
# MAGIC SELECT DISTINCT
# MAGIC     z.id AS cod_Comercial,
# MAGIC     z.full_name AS nombre_Comercial,
# MAGIC     z.role_name AS equipo_Comercial,
# MAGIC     z.email AS email,
# MAGIC     current_date() AS fecha_Desde,
# MAGIC     NULL AS fecha_Hasta
# MAGIC FROM silver_lakehouse.ZohoUsers_38b z;
# MAGIC
# MAGIC --select * from dim_comercial_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ Asegurar que el registro `-1` siempre existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_comercial AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS cod_comercial, 'n/a' AS nombre_comercial, 'n/a' AS equipo_comercial, 'n/a' AS email, NULL AS activo, NULL AS fecha_desde, NULL AS fecha_hasta
# MAGIC ) AS source
# MAGIC ON target.id_dim_comercial = -1  -- Solo se compara con `-1`
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (cod_comercial, nombre_comercial, equipo_comercial, email, activo, fecha_desde, fecha_hasta)
# MAGIC     VALUES (source.cod_comercial, source.nombre_comercial, source.equipo_comercial, source.email, source.activo, source.fecha_desde, source.fecha_hasta);

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
# MAGIC         --target.fecha_Hasta = CURRENT_DATE(),
# MAGIC         target.fecha_Hasta = CURRENT_DATE()-1,
# MAGIC         target.activo = 0;  -- Marcamos como inactivo
# MAGIC
# MAGIC -- 2️⃣ 🔹 Insertar nuevos registros para comerciales cerrados o inexistentes
# MAGIC INSERT INTO gold_lakehouse.dim_comercial (cod_Comercial, nombre_Comercial, equipo_Comercial, email, fecha_Desde, fecha_Hasta, activo)
# MAGIC SELECT 
# MAGIC     source.cod_Comercial,
# MAGIC     source.nombre_Comercial,
# MAGIC     source.equipo_Comercial,
# MAGIC     source.email,
# MAGIC     CURRENT_DATE(), -- La nueva fecha desde es la de hoy
# MAGIC     NULL, -- Se deja abierto hasta que vuelva a cambiar
# MAGIC     1  -- Activo
# MAGIC FROM dim_comercial_temp_view source
# MAGIC LEFT JOIN gold_lakehouse.dim_comercial target
# MAGIC ON COALESCE(source.cod_Comercial, '') = COALESCE(target.cod_Comercial, '')
# MAGIC AND target.fecha_Hasta IS NULL -- Nos aseguramos de comparar solo los registros abiertos
# MAGIC WHERE target.cod_Comercial IS NULL OR target.fecha_Hasta IS NOT NULL; -- 🔹 Insertar si no existe o si el último registro está cerrado
