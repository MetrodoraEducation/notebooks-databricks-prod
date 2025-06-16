# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_PRODUCTO**

# COMMAND ----------

# MAGIC %md
# MAGIC El código está preparando y limpiando datos relacionados con productos educativos antes de insertarlos en una dimensión de productos dentro de un sistema tipo Data Lakehouse. 
# MAGIC
# MAGIC Se centra en:
# MAGIC Asignación de claves de productos
# MAGIC Normalización y validación de datos
# MAGIC Conversión de formatos incorrectos
# MAGIC Creación de una vista que sirva como base para operaciones posteriores

# COMMAND ----------

# DBTITLE 1,Create view dim_producto
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_producto_view AS
# MAGIC SELECT DISTINCT
# MAGIC        enroll_group_id AS cod_Producto_Origen,
# MAGIC        enroll_alias AS cod_Producto_Corto,
# MAGIC        enroll_group_name AS cod_Producto,
# MAGIC        school_name AS origen_Producto,
# MAGIC        degree_title AS tipo_Producto,
# MAGIC        area_title AS area,
# MAGIC        nombre_del_programa_oficial_completo AS nombre_Oficial,
# MAGIC        ciclo_title AS curso,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(year) = '' THEN NULL
# MAGIC            ELSE TRY_CAST(year AS INT) 
# MAGIC        END AS numero_Curso,
# MAGIC        TO_DATE(fecha_inicio_docencia, 'dd/MM/yyyy') AS fecha_Inicio_Curso,
# MAGIC        TO_DATE(fecha_fin_docencia, 'dd/MM/yyyy') AS fecha_Fin_Curso,
# MAGIC        TO_DATE(fecha_inicio_reconocimiento_ingresos, 'dd/MM/yyyy') AS fecha_inicio_reconocimiento,
# MAGIC        TO_DATE(fecha_fin_reconocimiento_ingresos, 'dd/MM/yyyy') AS fecha_fin_reconocimiento,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(ciclo_id) = '' THEN NULL
# MAGIC            ELSE TRY_CAST(ciclo_id AS INT) 
# MAGIC        END AS ciclo_id,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(plazas) = '' THEN 0
# MAGIC            ELSE TRY_CAST(plazas AS INT) 
# MAGIC        END AS num_Plazas,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(grupo) = '' THEN 0
# MAGIC            ELSE TRY_CAST(grupo AS INT) 
# MAGIC        END AS num_Grupo,
# MAGIC        group_vertical AS vertical,
# MAGIC        codigo_vertical AS cod_Vertical,
# MAGIC        especialidad,
# MAGIC        codigo_especialidad AS cod_Especialidad,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(creditos) = '' THEN 0
# MAGIC            ELSE TRY_CAST(creditos AS DOUBLE) 
# MAGIC        END AS num_Creditos,
# MAGIC        UPPER(codigo_programa) AS cod_Programa,
# MAGIC        admisionsino AS admite_Admision,
# MAGIC        tiponegocio AS tipo_Negocio,
# MAGIC        acreditado,
# MAGIC        nombreweb AS nombre_Web,
# MAGIC        --Cambio para usar el campo group y mapear con la nueva tabla de mapeo entidad_legal
# MAGIC        --area_entidad_legal AS entidad_Legal,
# MAGIC        group_entidad_legal AS entidad_Legal,
# MAGIC        codigo_entidad_legal AS cod_Entidad_Legal,
# MAGIC        section_title AS modalidad,
# MAGIC        modalidad_code AS cod_Modalidad,
# MAGIC        TRY_CAST(codigo_sede AS FLOAT) AS codigo_sede,
# MAGIC        TRIM(UPPER(group_sede)) AS sede,
# MAGIC        section_title AS section_title,
# MAGIC        TO_DATE(fecha_inicio, 'dd/MM/yyyy') AS fecha_inicio,
# MAGIC        TO_DATE(fecha_Fin, 'dd/MM/yyyy') AS fecha_Fin,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(meses_duracion) = '' THEN 0
# MAGIC            ELSE TRY_CAST(meses_duracion AS INT) 
# MAGIC        END AS meses_Duracion,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(horas_acreditadas) = '' THEN 0
# MAGIC            ELSE TRY_CAST(horas_acreditadas AS INT) 
# MAGIC        END AS horas_Acreditadas,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(horas_presenciales) = '' THEN 0
# MAGIC            ELSE TRY_CAST(horas_presenciales AS INT) 
# MAGIC        END AS horas_Presenciales,
# MAGIC        TRY_CAST(fecha_inicio_pago AS DATE) AS fecha_inicio_pago,
# MAGIC        TRY_CAST(fecha_fin_pago AS DATE) AS fecha_fin_pago,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(cuotas_docencia) = '' THEN 0
# MAGIC            ELSE TRY_CAST(cuotas_docencia AS INT) 
# MAGIC        END AS num_Cuotas,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(tarifa_euneiz) = '' THEN 0.0
# MAGIC            ELSE TRY_CAST(tarifa_euneiz AS DOUBLE) 
# MAGIC        END AS importe_Certificado,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(tarifa_ampliacion) = '' THEN 0.0
# MAGIC            ELSE TRY_CAST(tarifa_ampliacion AS DOUBLE) 
# MAGIC        END AS importe_Ampliacion,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(tarifa_docencia) = '' THEN 0.0
# MAGIC            ELSE TRY_CAST(tarifa_docencia AS DOUBLE) 
# MAGIC        END AS importe_Docencia,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(tarifa_matricula) = '' THEN 0.0
# MAGIC            ELSE TRY_CAST(tarifa_matricula AS DOUBLE) 
# MAGIC        END AS importe_Matricula,
# MAGIC        COALESCE(TRY_CAST(tarifa_docencia AS DOUBLE), 0.0) +
# MAGIC        COALESCE(TRY_CAST(tarifa_matricula AS DOUBLE), 0.0) AS importe_Total,
# MAGIC        TRY_CAST(fecha_creacion AS TIMESTAMP) AS fecha_creacion,
# MAGIC        TRY_CAST(ultima_actualizacion AS TIMESTAMP) AS ultima_actualizacion,
# MAGIC        CURRENT_TIMESTAMP() AS ETLcreatedDate,
# MAGIC        CURRENT_TIMESTAMP() AS ETLupdatedDate
# MAGIC FROM silver_lakehouse.classlifetitulaciones
# MAGIC WHERE enroll_group_name IS NOT NULL
# MAGIC
# MAGIC UNION ALL--Union ALL classlifetitulaciones_931
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC        enroll_group_id AS cod_Producto_Origen,
# MAGIC        enroll_alias AS cod_Producto_Corto,
# MAGIC        enroll_group_name AS cod_Producto,
# MAGIC        school_name AS origen_Producto,
# MAGIC        degree_title AS tipo_Producto,
# MAGIC        area_title AS area,
# MAGIC        nombre_del_programa_oficial_completo AS nombre_Oficial,
# MAGIC        ciclo_title AS curso,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(year) = '' THEN NULL
# MAGIC            ELSE TRY_CAST(year AS INT) 
# MAGIC        END AS numero_Curso,
# MAGIC        TO_DATE(fecha_inicio_docencia, 'dd/MM/yyyy') AS fecha_Inicio_Curso,
# MAGIC        TO_DATE(fecha_fin_docencia, 'dd/MM/yyyy') AS fecha_Fin_Curso,
# MAGIC        TO_DATE(fecha_inicio_reconocimiento_ingresos, 'dd/MM/yyyy') AS fecha_inicio_reconocimiento,
# MAGIC        TO_DATE(fecha_fin_reconocimiento_ingresos, 'dd/MM/yyyy') AS fecha_fin_reconocimiento,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(ciclo_id) = '' THEN NULL
# MAGIC            ELSE TRY_CAST(ciclo_id AS INT) 
# MAGIC        END AS ciclo_id,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(plazas) = '' THEN 0
# MAGIC            ELSE TRY_CAST(plazas AS INT) 
# MAGIC        END AS num_Plazas,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(grupo) = '' THEN 0
# MAGIC            ELSE TRY_CAST(grupo AS INT) 
# MAGIC        END AS num_Grupo,
# MAGIC        group_vertical AS vertical,
# MAGIC        codigo_vertical AS cod_Vertical,
# MAGIC        especialidad,
# MAGIC        codigo_especialidad AS cod_Especialidad,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(creditos) = '' THEN 0
# MAGIC            ELSE TRY_CAST(creditos AS DOUBLE) 
# MAGIC        END AS num_Creditos,
# MAGIC        UPPER(codigo_programa) AS cod_Programa,
# MAGIC        admisionsino AS admite_Admision,
# MAGIC        tiponegocio AS tipo_Negocio,
# MAGIC        acreditado,
# MAGIC        nombreweb AS nombre_Web,
# MAGIC        --Cambio para usar el campo group y mapear con la nueva tabla de mapeo entidad_legal
# MAGIC        --area_entidad_legal AS entidad_Legal,
# MAGIC        group_entidad_legal AS entidad_Legal,
# MAGIC        codigo_entidad_legal AS cod_Entidad_Legal,
# MAGIC        term_title AS modalidad, --section_title
# MAGIC        modalidad_code AS cod_Modalidad,
# MAGIC        TRY_CAST(codigo_sede AS FLOAT) AS codigo_sede,
# MAGIC        TRIM(UPPER(group_sede)) AS sede,
# MAGIC        section_title AS section_title,
# MAGIC        TO_DATE(fecha_inicio, 'dd/MM/yyyy') AS fecha_inicio,
# MAGIC        TO_DATE(fecha_Fin, 'dd/MM/yyyy') AS fecha_Fin,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(meses_duracion) = '' THEN 0
# MAGIC            ELSE TRY_CAST(meses_duracion AS INT) 
# MAGIC        END AS meses_Duracion,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(horas_acreditadas) = '' THEN 0
# MAGIC            ELSE TRY_CAST(horas_acreditadas AS INT) 
# MAGIC        END AS horas_Acreditadas,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(horas_presenciales) = '' THEN 0
# MAGIC            ELSE TRY_CAST(horas_presenciales AS INT) 
# MAGIC        END AS horas_Presenciales,
# MAGIC        TRY_CAST(fecha_inicio_pago AS DATE) AS fecha_inicio_pago,
# MAGIC        TRY_CAST(fecha_fin_pago AS DATE) AS fecha_fin_pago,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(cuotas_docencia) = '' THEN 0
# MAGIC            ELSE TRY_CAST(cuotas_docencia AS INT) 
# MAGIC        END AS num_Cuotas,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(tarifa_euneiz) = '' THEN 0.0
# MAGIC            ELSE TRY_CAST(tarifa_euneiz AS DOUBLE) 
# MAGIC        END AS importe_Certificado,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(tarifa_ampliacion) = '' THEN 0.0
# MAGIC            ELSE TRY_CAST(tarifa_ampliacion AS DOUBLE) 
# MAGIC        END AS importe_Ampliacion,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(tarifa_docencia) = '' THEN 0.0
# MAGIC            ELSE TRY_CAST(tarifa_docencia AS DOUBLE) 
# MAGIC        END AS importe_Docencia,
# MAGIC        CASE 
# MAGIC            WHEN TRIM(tarifa_matricula) = '' THEN 0.0
# MAGIC            ELSE TRY_CAST(tarifa_matricula AS DOUBLE) 
# MAGIC        END AS importe_Matricula,
# MAGIC        COALESCE(TRY_CAST(tarifa_docencia AS DOUBLE), 0.0) +
# MAGIC        COALESCE(TRY_CAST(tarifa_matricula AS DOUBLE), 0.0) AS importe_Total,
# MAGIC        TRY_CAST(fecha_creacion AS TIMESTAMP) AS fecha_creacion,
# MAGIC        TRY_CAST(ultima_actualizacion AS TIMESTAMP) AS ultima_actualizacion,
# MAGIC        CURRENT_TIMESTAMP() AS ETLcreatedDate,
# MAGIC        CURRENT_TIMESTAMP() AS ETLupdatedDate
# MAGIC FROM silver_lakehouse.classlifetitulaciones_931;

# COMMAND ----------

# DBTITLE 1,Count duplicate view source
# MAGIC %sql
# MAGIC SELECT cod_Producto, COUNT(*)
# MAGIC FROM dim_producto_view
# MAGIC GROUP BY cod_Producto 
# MAGIC HAVING COUNT(*) > 1; --1061

# COMMAND ----------

#%sql select * from dim_producto_view where cod_producto = 'FCMLPT04-FF119101P-MAD2510-1'

# COMMAND ----------

# DBTITLE 1,Merge dim_producto
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_producto AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         '-1' AS cod_Producto_Origen, 'n/a' AS cod_Producto_Corto, 'n/a' AS cod_Producto, 'n/a' AS origen_Producto,
# MAGIC         'n/a' AS tipo_Producto, 'n/a' AS area, 'n/a' AS nombre_Oficial, 'n/a' AS curso, 0 AS numero_Curso, 0 AS codigo_sede, 'n/a' AS sede,
# MAGIC         '1900-01-01' AS fecha_Inicio_Curso, '1900-01-01' AS fecha_Fin_Curso,'1900-01-01' AS fecha_inicio_reconocimiento, '1900-01-01' AS fecha_fin_reconocimiento, 0 AS ciclo_id, 0 AS num_Plazas,
# MAGIC         0 AS num_Grupo, 'n/a' AS vertical, 'n/a' AS cod_Vertical, 'n/a' AS especialidad, 'n/a' AS cod_Especialidad,
# MAGIC         0.0 AS num_Creditos, 'n/a' AS cod_Programa, 'n/a' AS admite_Admision, 'n/a' AS tipo_Negocio, 'n/a' AS acreditado,
# MAGIC         'n/a' AS nombre_Web, 'n/a' AS entidad_Legal, 'n/a' AS cod_Entidad_Legal, 'n/a' AS modalidad, 'n/a' AS cod_Modalidad,
# MAGIC         '1900-01-01' AS fecha_Inicio, '1900-01-01' AS fecha_Fin, 0 AS meses_Duracion, 0 AS horas_Acreditadas, 0 AS horas_Presenciales,
# MAGIC         '1900-01-01' AS fecha_Inicio_Pago, '1900-01-01' AS fecha_Fin_Pago, 0 AS num_Cuotas, 0.0 AS importe_Certificado,
# MAGIC         0.0 AS importe_Ampliacion, 0.0 AS importe_Docencia, 0.0 AS importe_Matricula, 0.0 AS importe_Total,
# MAGIC         CURRENT_TIMESTAMP AS ETLupdatedDate, CURRENT_TIMESTAMP AS ETLcreatedDate
# MAGIC ) AS source
# MAGIC ON target.id_Dim_Producto = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         cod_Producto_Origen, cod_Producto_Corto, cod_Producto, origen_Producto, tipo_Producto, area, nombre_Oficial, curso, numero_Curso, codigo_sede, sede, fecha_Inicio_Curso, fecha_Fin_Curso, fecha_inicio_reconocimiento,fecha_fin_reconocimiento, ciclo_id, num_Plazas, num_Grupo, vertical, cod_Vertical, especialidad, cod_Especialidad, num_Creditos, cod_Programa, admite_Admision, tipo_Negocio, acreditado, nombre_Web, entidad_Legal, cod_Entidad_Legal, modalidad, cod_Modalidad, fecha_Inicio, fecha_Fin, meses_Duracion, horas_Acreditadas, horas_Presenciales, fecha_Inicio_Pago, fecha_Fin_Pago, num_Cuotas, importe_Certificado, importe_Ampliacion, importe_Docencia, importe_Matricula, importe_Total, ETLcreatedDate, ETLupdatedDate
# MAGIC     ) VALUES (
# MAGIC         source.cod_Producto_Origen, source.cod_Producto_Corto, source.cod_Producto, source.origen_Producto, source.tipo_Producto, source.area, 
# MAGIC         source.nombre_Oficial, source.curso, source.numero_Curso, source.codigo_sede, source.sede, source.fecha_Inicio_Curso, source.fecha_Fin_Curso, source.fecha_inicio_reconocimiento, source.fecha_fin_reconocimiento, source.ciclo_id, source.num_Plazas, 
# MAGIC         source.num_Grupo, source.vertical, source.cod_Vertical, source.especialidad, source.cod_Especialidad, source.num_Creditos, source.cod_Programa, 
# MAGIC         source.admite_Admision, source.tipo_Negocio, source.acreditado, source.nombre_Web, source.entidad_Legal, source.cod_Entidad_Legal, source.modalidad, 
# MAGIC         source.cod_Modalidad, source.fecha_Inicio, source.fecha_Fin, source.meses_Duracion, source.horas_Acreditadas, source.horas_Presenciales, 
# MAGIC         source.fecha_Inicio_Pago, source.fecha_Fin_Pago, source.num_Cuotas, source.importe_Certificado, source.importe_Ampliacion, source.importe_Docencia, 
# MAGIC         source.importe_Matricula, source.importe_Total, source.ETLcreatedDate, source.ETLupdatedDate
# MAGIC     );
# MAGIC
# MAGIC -- 2️⃣ MERGE principal para `dim_producto`, evitando modificar el registro `-1`
# MAGIC MERGE INTO gold_lakehouse.dim_producto AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT * FROM dim_producto_view 
# MAGIC     WHERE cod_Producto_Origen <> '-1'
# MAGIC ) AS source
# MAGIC ON target.cod_Producto = source.cod_Producto
# MAGIC AND target.id_Dim_Producto != -1
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.origen_Producto IS DISTINCT FROM source.origen_Producto OR
# MAGIC     target.tipo_Producto IS DISTINCT FROM source.tipo_Producto OR
# MAGIC     target.area IS DISTINCT FROM source.area OR
# MAGIC     target.nombre_Oficial IS DISTINCT FROM source.nombre_Oficial OR
# MAGIC     target.curso IS DISTINCT FROM source.curso OR
# MAGIC     target.numero_Curso IS DISTINCT FROM source.numero_Curso OR
# MAGIC     target.codigo_sede IS DISTINCT FROM source.codigo_sede OR
# MAGIC     target.sede IS DISTINCT FROM source.sede OR
# MAGIC     target.ciclo_id IS DISTINCT FROM source.ciclo_id OR
# MAGIC     target.num_Plazas IS DISTINCT FROM source.num_Plazas OR
# MAGIC     target.num_Grupo IS DISTINCT FROM source.num_Grupo OR
# MAGIC     target.vertical IS DISTINCT FROM source.vertical OR
# MAGIC     target.cod_Vertical IS DISTINCT FROM source.cod_Vertical OR
# MAGIC     target.especialidad IS DISTINCT FROM source.especialidad OR
# MAGIC     target.cod_Especialidad IS DISTINCT FROM source.cod_Especialidad OR
# MAGIC     target.num_Creditos IS DISTINCT FROM source.num_Creditos OR
# MAGIC     target.cod_Programa IS DISTINCT FROM source.cod_Programa OR
# MAGIC     target.admite_Admision IS DISTINCT FROM source.admite_Admision OR
# MAGIC     target.tipo_Negocio IS DISTINCT FROM source.tipo_Negocio OR
# MAGIC     target.acreditado IS DISTINCT FROM source.acreditado OR
# MAGIC     target.nombre_Web IS DISTINCT FROM source.nombre_Web OR
# MAGIC     target.entidad_Legal IS DISTINCT FROM source.entidad_Legal OR
# MAGIC     target.cod_Entidad_Legal IS DISTINCT FROM source.cod_Entidad_Legal OR
# MAGIC     target.modalidad IS DISTINCT FROM source.modalidad OR
# MAGIC     target.cod_Modalidad IS DISTINCT FROM source.cod_Modalidad OR
# MAGIC     target.fecha_Inicio IS DISTINCT FROM source.fecha_Inicio OR
# MAGIC     target.fecha_Fin IS DISTINCT FROM source.fecha_Fin OR
# MAGIC     target.meses_Duracion IS DISTINCT FROM source.meses_Duracion OR
# MAGIC     target.horas_Acreditadas IS DISTINCT FROM source.horas_Acreditadas OR
# MAGIC     target.horas_Presenciales IS DISTINCT FROM source.horas_Presenciales OR
# MAGIC     target.fecha_Inicio_Pago IS DISTINCT FROM source.fecha_Inicio_Pago OR
# MAGIC     target.fecha_Fin_Pago IS DISTINCT FROM source.fecha_Fin_Pago OR
# MAGIC     target.num_Cuotas IS DISTINCT FROM source.num_Cuotas OR
# MAGIC     target.importe_Certificado IS DISTINCT FROM source.importe_Certificado OR
# MAGIC     target.importe_Ampliacion IS DISTINCT FROM source.importe_Ampliacion OR
# MAGIC     target.importe_Docencia IS DISTINCT FROM source.importe_Docencia OR
# MAGIC     target.importe_Matricula IS DISTINCT FROM source.importe_Matricula OR
# MAGIC     target.importe_Total IS DISTINCT FROM source.importe_Total OR
# MAGIC     target.fecha_Inicio_Curso IS DISTINCT FROM source.fecha_Inicio_Curso OR
# MAGIC     target.fecha_Fin_Curso IS DISTINCT FROM source.fecha_Fin_Curso OR
# MAGIC     target.Fecha_Inicio_Reconocimiento IS DISTINCT FROM source.Fecha_Inicio_Reconocimiento OR
# MAGIC     target.Fecha_Fin_Reconocimiento IS DISTINCT FROM source.Fecha_Fin_Reconocimiento
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.origen_Producto = source.origen_Producto,
# MAGIC     target.tipo_Producto = source.tipo_Producto,
# MAGIC     target.area = source.area,
# MAGIC     target.nombre_Oficial = source.nombre_Oficial,
# MAGIC     target.curso = source.curso,
# MAGIC     target.numero_Curso = source.numero_Curso,
# MAGIC     target.codigo_sede = source.codigo_sede,
# MAGIC     target.sede = source.sede,
# MAGIC     target.ciclo_id = source.ciclo_id,
# MAGIC     target.num_Plazas = source.num_Plazas,
# MAGIC     target.num_Grupo = source.num_Grupo,
# MAGIC     target.vertical = source.vertical,
# MAGIC     target.cod_Vertical = source.cod_Vertical,
# MAGIC     target.especialidad = source.especialidad,
# MAGIC     target.cod_Especialidad = source.cod_Especialidad,
# MAGIC     target.num_Creditos = source.num_Creditos,
# MAGIC     target.cod_Programa = source.cod_Programa,
# MAGIC     target.admite_Admision = source.admite_Admision,
# MAGIC     target.tipo_Negocio = source.tipo_Negocio,
# MAGIC     target.acreditado = source.acreditado,
# MAGIC     target.nombre_Web = source.nombre_Web,
# MAGIC     target.entidad_Legal = source.entidad_Legal,
# MAGIC     target.cod_Entidad_Legal = source.cod_Entidad_Legal,
# MAGIC     target.modalidad = source.modalidad,
# MAGIC     target.cod_Modalidad = source.cod_Modalidad,
# MAGIC     target.fecha_Inicio = source.fecha_Inicio,
# MAGIC     target.fecha_Fin = source.fecha_Fin,
# MAGIC     target.meses_Duracion = source.meses_Duracion,
# MAGIC     target.horas_Acreditadas = source.horas_Acreditadas,
# MAGIC     target.horas_Presenciales = source.horas_Presenciales,
# MAGIC     target.fecha_Inicio_Pago = source.fecha_Inicio_Pago,
# MAGIC     target.fecha_Fin_Pago = source.fecha_Fin_Pago,
# MAGIC     target.num_Cuotas = source.num_Cuotas,
# MAGIC     target.importe_Certificado = source.importe_Certificado,
# MAGIC     target.importe_Ampliacion = source.importe_Ampliacion,
# MAGIC     target.importe_Docencia = source.importe_Docencia,
# MAGIC     target.importe_Matricula = source.importe_Matricula,
# MAGIC     target.importe_Total = source.importe_Total,
# MAGIC     target.fecha_Inicio_Curso = source.fecha_Inicio_Curso,
# MAGIC     target.fecha_Fin_Curso = source.fecha_Fin_Curso,
# MAGIC     target.Fecha_Inicio_Reconocimiento = source.Fecha_Inicio_Reconocimiento,
# MAGIC     target.Fecha_Fin_Reconocimiento = source.Fecha_Fin_Reconocimiento,
# MAGIC     target.ETLupdatedDate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         cod_Producto_Origen, cod_Producto_Corto, cod_Producto, origen_Producto, tipo_Producto, area, nombre_Oficial, curso, numero_Curso, codigo_sede, sede,
# MAGIC         fecha_Inicio_Curso, fecha_Fin_Curso, fecha_inicio_reconocimiento,fecha_fin_reconocimiento, ciclo_id, num_Plazas, num_Grupo, vertical, cod_Vertical, especialidad, cod_Especialidad, num_Creditos, 
# MAGIC         cod_Programa, admite_Admision, tipo_Negocio, acreditado, nombre_Web, entidad_Legal, cod_Entidad_Legal, modalidad, cod_Modalidad, fecha_Inicio, 
# MAGIC         fecha_Fin, meses_Duracion, horas_Acreditadas, horas_Presenciales, fecha_Inicio_Pago, fecha_Fin_Pago, num_Cuotas, importe_Certificado, 
# MAGIC         importe_Ampliacion, importe_Docencia, importe_Matricula, importe_Total, ETLcreatedDate, ETLupdatedDate
# MAGIC     ) VALUES (
# MAGIC         source.cod_Producto_Origen, source.cod_Producto_Corto, source.cod_Producto, source.origen_Producto, source.tipo_Producto, source.area, 
# MAGIC         source.nombre_Oficial, source.curso, source.numero_Curso, source.codigo_sede, source.sede, source.fecha_Inicio_Curso, source.fecha_Fin_Curso, source.fecha_inicio_reconocimiento, source.fecha_fin_reconocimiento, source.ciclo_id, source.num_Plazas, source.num_Grupo, source.vertical, source.cod_Vertical, source.especialidad, source.cod_Especialidad, source.num_Creditos, source.cod_Programa, source.admite_Admision, source.tipo_Negocio, source.acreditado, source.nombre_Web, source.entidad_Legal, source.cod_Entidad_Legal, source.modalidad, source.cod_Modalidad, source.fecha_Inicio, source.fecha_Fin, source.meses_Duracion, source.horas_Acreditadas, source.horas_Presenciales, source.fecha_Inicio_Pago, source.fecha_Fin_Pago, source.num_Cuotas, source.importe_Certificado, source.importe_Ampliacion, source.importe_Docencia, source.importe_Matricula, source.importe_Total, source.ETLcreatedDate, source.ETLupdatedDate
# MAGIC     );

# COMMAND ----------

# DBTITLE 1,Validate duplicates >1
# MAGIC %sql
# MAGIC SELECT cod_Producto, COUNT(*) AS total_duplicados
# MAGIC FROM gold_lakehouse.dim_producto
# MAGIC GROUP BY cod_Producto
# MAGIC HAVING COUNT(*) > 1;
