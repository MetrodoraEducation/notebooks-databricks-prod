# Databricks notebook source
# MAGIC %md
# MAGIC **Database Gold**

# COMMAND ----------

# DBTITLE 1,Create database gold_lakehouse
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold_lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC **Definición Storage Account**

# COMMAND ----------

# DBTITLE 1,Declare variable storage_account_name
storage_account_name = "stmetrodoralakehousepro"

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla fct_llamada**

# COMMAND ----------

# DBTITLE 1,Create or replace fct_llamada

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.fct_llamada (
    id_llamada BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1), 
    cod_llamada STRING,
    id_dim_tiempo DATE,
    id_dim_hora STRING,
    direccion_llamada STRING,
    duration INT,
    numero_telefono STRING,
    id_dim_pais INT,
    id_dim_comercial INT,
    id_dim_motivo_perdida_llamada INT,
    processdate TIMESTAMP
)   
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/fct_llamada';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_fecha**

# COMMAND ----------

# DBTITLE 1,Create or replace dim_fecha

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.dim_fecha (
    id_dim_fecha DATE, 
    fecha_larga DATE, 
    dia_semana STRING,
    dia_semana_corto STRING, 
    dia_semana_numero INT, 
    mes_numero INT, 
    mes_corto STRING, 
    mes_largo STRING, 
    dia_anio INT,
    semana_anio INT,
    numero_dias_mes INT, 
    dia_mes INT,
    primer_dia_mes DATE, 
    ultimo_dia_mes DATE, 
    anio_numero INT,
    trimestre_numero INT, 
    trimestre_nombre STRING, 
    mes_fiscal_numero INT, 
    anio_fiscal_numero INT,
    curso_academico STRING, 
    es_laborable BOOLEAN, 
    es_finde_semana BOOLEAN 
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_fecha';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_hora**

# COMMAND ----------

# DBTITLE 1,Create or replace dim_hora

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.dim_hora (
    id_dim_hora INT, 
    id_dim_hora_larga STRING,
    hora INT, 
    minuto INT, 
    hora12 INT, 
    pmam STRING 
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_hora';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_pais**

# COMMAND ----------

# DBTITLE 1,Create or replace dim_pais

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.dim_pais (
    id int,
    nombre string,
    name string,
    nombre_nacionalidad string,
    iso2 string,	
    iso3 string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_pais';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_motivo_perdida_llamada**

# COMMAND ----------

# DBTITLE 1,Create or replace dim_motivo_perdida_llamada

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.dim_motivo_perdida_llamada (
    id_dim_motivo_perdida_llamada BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),    
    motivo_perdida_llamada STRING,
    tipo_perdida STRING
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_motivo_perdida_llamada';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_comercial**

# COMMAND ----------

# DBTITLE 1,Create or replace dim_comercial

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.dim_comercial (
    id_dim_comercial BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    cod_comercial STRING, 
    nombre_comercial STRING,
    equipo_comercial STRING,
    email STRING,    
    activo INTEGER,
    fecha_desde DATE,
    fecha_hasta DATE
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_comercial';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla Mapeo Origen _Campania**

# COMMAND ----------

# DBTITLE 1,mapeo_origen_campania

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.mapeo_origen_campania
(
utm_source string, 
utm_type string, 
utm_channel string, 
utm_medium string 
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_origen_campania';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_origen_campania**

# COMMAND ----------

# DBTITLE 1,dim_origen_campania
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_origen_campania
(
    id_dim_origen_campania BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_origen_campania string, 
    tipo_campania string, 
    canal_campania string, 
    medio_campania string,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP,
    fec_procesamiento TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_origen_campania';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_campania**

# COMMAND ----------

# DBTITLE 1,dim_campania

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_campania
(
id_dim_campania BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
nombre_campania string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_campania';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_estado_venta**

# COMMAND ----------

# DBTITLE 1,dim_estado_venta

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_estado_venta
(
    id_dim_estado_venta BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_estado_venta STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_estado_venta';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_etapa_venta**

# COMMAND ----------

# DBTITLE 1,dim_etapa_venta

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_etapa_venta
(
    id_dim_etapa_venta BIGINT,
    orden_etapa INT,
    nombre_etapa_venta STRING,
    nombreEtapaVentaAgrupado STRING,
    esNe BIGINT,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_etapa_venta';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla mapeo_modalidad**

# COMMAND ----------

# DBTITLE 1,mapeo_modalidad

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.mapeo_modalidad
(
modalidad string, 
modalidad_norm string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_modalidad';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_modalidad**

# COMMAND ----------

# DBTITLE 1,dim_modalidad

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_modalidad
(
        id_dim_modalidad BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
        nombre_modalidad STRING,
        ETLcreatedDate TIMESTAMP,
        ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_modalidad';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_institucion**

# COMMAND ----------

# DBTITLE 1,dim_institucion

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_institucion
(
    id_dim_institucion BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_institucion STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_institucion';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla mapeo_sede**

# COMMAND ----------

# DBTITLE 1,mapeo_sede

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.mapeo_sede
(
sede string, 
sede_norm string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_sede';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_sede**

# COMMAND ----------

# DBTITLE 1,dim_sede

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_sede
(
    id_dim_sede BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    number_codigo_sede BIGINT,
    nombre_sede STRING,
    codigo_sede STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_sede';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla mapeo_estudio**

# COMMAND ----------

# DBTITLE 1,mapeo_estudio

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.mapeo_estudio
(
estudio string, 
estudio_norm string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_estudio';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_estudio**

# COMMAND ----------

# DBTITLE 1,dim_estudio

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_estudio
(
    id_dim_estudio BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    cod_estudio string,
    nombre_de_programa string,
    cod_vertical string,
    vertical_desc string,
    cod_entidad_legal string,
    entidad_legal_desc string,
    cod_especialidad string,
    especialidad_desc string,
    cod_tipo_formacion string,
    tipo_formacion_desc string,
    cod_tipo_negocio string,
    tipo_negocio_desc string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_estudio';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_localidad**

# COMMAND ----------

# DBTITLE 1,dim_localidad

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_localidad
(
    id_dim_localidad BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_localidad STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_localidad';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_motivo_cierre**

# COMMAND ----------

# DBTITLE 1,dim_motivo_cierre

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_motivo_cierre
(
    id_dim_motivo_cierre BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    motivo_cierre STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_motivo_cierre';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_tipo_formacion**

# COMMAND ----------

# DBTITLE 1,dim_tipo_formacion

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_tipo_formacion
(
    id_dim_tipo_formacion BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    tipo_formacion_desc STRING,
    cod_tipo_formacion STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_tipo_formacion';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_tipo_negocio**

# COMMAND ----------

# DBTITLE 1,dim_tipo_negocio

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_tipo_negocio
(
    id_dim_tipo_negocio BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    tipo_negocio_desc STRING,
    cod_tipo_negocio STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_tipo_negocio';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla fct_venta**

# COMMAND ----------

# DBTITLE 1,fct_venta

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.fct_venta
(
    id_venta BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    cod_venta string,
    nombre string,
    email string,
    telefono string,
    nombre_contacto string,
    id_dim_propietario_lead BIGINT,
    id_dim_origen_campania BIGINT,
    id_dim_campania BIGINT,
    importe_venta double,
    importe_descuento double,
    importe_venta_neta double,
    id_dim_estado_venta BIGINT,
    id_dim_etapa_venta BIGINT,
    posibilidad_venta double,
    fec_creacion date,
    fec_modificacion date,
    fec_cierre date,
    id_dim_modalidad BIGINT,
    id_dim_institucion BIGINT,
    id_dim_sede BIGINT,
    id_dim_pais INT,
    id_dim_estudio BIGINT,
    fec_pago_matricula date,
    importe_matricula double, 
    importe_descuento_matricula double,
    importe_neto_matricula double,
    id_dim_localidad BIGINT,
    id_dim_tipo_formacion BIGINT,
    id_dim_tipo_negocio BIGINT,
    nombre_scoring string,
    puntos_scoring double,
    dias_cierre int,
    id_dim_motivo_cierre BIGINT,
    fec_procesamiento timestamp,
    sistema_origen string,
    tiempo_de_maduracion int,
    new_enrollent INT,
    lead_neto INT,
    activo INT,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/fct_venta';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_escenario_budget**

# COMMAND ----------

# DBTITLE 1,dim_escenario_budget

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_escenario_budget
(
id_dim_escenario_budget BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
escenario string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_escenario_budget';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_titulacion_budget**

# COMMAND ----------

# DBTITLE 1,dim_titulacion_budget

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_titulacion_budget
(
id_dim_titulacion_budget BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
titulacion string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_titulacion_budget';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla fct_budget_old**

# COMMAND ----------

# DBTITLE 1,old_fct_budget

###sql_query = f"""
###CREATE TABLE IF NOT EXISTS gold_lakehouse.fct_budget
###(
###id_budget BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
###fec_budget date,
###id_dim_escenario_budget	BIGINT,
###id_dim_titulacion_budget BIGINT,
###centro string,
###sede string,
###modalidad string,	
###num_leads_netos	integer,
###num_leads_brutos integer,
###num_matriculas integer,
###importe_venta_neta double,
###importe_venta_bruta double,
###importe_captacion	double,
###fec_procesamiento timestamp
###)
###USING DELTA
###LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/fct_budget';
###"""
###
###spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tables Zoho**

# COMMAND ----------

# DBTITLE 1,dim_producto
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_producto
(
    id_Dim_Producto BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    cod_Producto_Origen STRING,
    cod_Producto_Corto STRING,
    cod_Producto STRING,
    origen_Producto STRING,
    tipo_Producto STRING,
    area STRING,
    nombre_Oficial STRING,
    curso STRING,
    numero_Curso INT,
    codigo_sede BIGINT,
    sede STRING,
    ciclo_id INT,
    num_Plazas INT,
    num_Grupo INT,
    vertical STRING,
    cod_Vertical STRING,
    especialidad STRING,
    cod_Especialidad STRING,
    num_Creditos DOUBLE,
    cod_Programa STRING,
    admite_Admision STRING,
    tipo_Negocio STRING,
    acreditado STRING,
    nombre_Web STRING,
    entidad_Legal STRING,
    cod_Entidad_Legal STRING,
    modalidad STRING,
    cod_Modalidad STRING,
    fecha_Inicio DATE,
    fecha_Fin DATE,
    meses_Duracion INT,
    horas_Acreditadas INT,
    horas_Presenciales INT,
    fecha_Inicio_Pago DATE,
    fecha_Fin_Pago DATE,
    num_Cuotas INT,
    importe_Certificado DOUBLE,
    importe_Ampliacion DOUBLE,
    importe_Docencia DOUBLE,
    importe_Matricula DOUBLE,
    importe_Total DOUBLE,
    fecha_Inicio_Curso DATE,
    fecha_Fin_Curso DATE,
    Fecha_Inicio_Reconocimiento DATE,
    Fecha_Fin_Reconocimiento DATE,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_producto';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_programa
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_programa
(
    id_Dim_Programa BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    cod_Programa STRING NOT NULL,
    nombre_Programa STRING NOT NULL,
    tipo_Programa STRING,
    entidad_Legal STRING,
    especialidad STRING,
    vertical STRING,
    nombre_Programa_Completo STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_programa';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_vertical
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_vertical 
(
    id_Dim_Vertical BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_Vertical STRING,
    nombre_Vertical_Corto STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_vertical ';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_entidad_legal
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_entidad_legal 
(
    id_Dim_Institucion BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_Institucion STRING NOT NULL,
    codigo_Entidad_Legal STRING NOT NULL,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_entidad_legal ';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_motivo_perdida
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_motivo_perdida 
(
    id_Dim_Motivo_Perdida BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_Dim_Motivo_Perdida STRING,
    cantidad STRING,
    calidad STRING,
    esBruto BIGINT,
    esNeto BIGINT,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_motivo_perdida';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_especialidad
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_especialidad 
(
    id_Dim_Especialidad BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1),
    nombre_Especialidad STRING NOT NULL,
    cod_Especialidad STRING NOT NULL,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_especialidad';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_utm_campaign
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_utm_campaign 
(
    id_dim_utm_campaign BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1) PRIMARY KEY,
    utm_campaign_id STRING NOT NULL,
    utm_campaign_name STRING,
    utm_strategy STRING,
    utm_channel STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_utm_campaign';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_utm_adset
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_utm_adset 
(
    id_dim_utm_ad BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1) PRIMARY KEY,
    utm_ad_id STRING NOT NULL,
    utm_adset_id STRING,
    utm_term STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_utm_adset';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_utm_source
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_utm_source 
(
    id_dim_utm_source BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1) PRIMARY KEY,
    utm_source STRING NOT NULL,
    utm_type STRING,
    utm_medium STRING,
    utm_profile STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_utm_source';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_nacionalidad

sql_query = f"""
CREATE OR REPLACE TABLE gold_lakehouse.dim_nacionalidad (
    id int,
    nombre string,
    name string,
    nombre_nacionalidad string,
    iso2 string,	
    iso3 string
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_nacionalidad';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,fctventa Zoho

sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.fctventa
(
    id_venta BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1),
    cod_lead STRING,
    cod_oportunidad STRING,
    nombre string,
    email string,
    telefono string,
    nombre_contacto string,
    importe_venta double,
    importe_descuento double,
    importe_venta_neto double,
    posibilidad_venta double,
    ciudad string,
    provincia string,
    calle string,
    codigo_postal string,
    nombre_scoring string,
    puntos_scoring double,
    dias_cierre int,
    fec_creacion date,
    fec_modificacion date,
    fec_cierre date,
    fec_pago_matricula date,
    fecha_hora_anulacion TIMESTAMP,
    fecha_Modificacion_Lead TIMESTAMP,
    fecha_Modificacion_Oportunidad TIMESTAMP,
    importe_matricula double,
    importe_descuento_matricula double,
    importe_neto_matricula double,
    kpi_new_enrollent BIGINT,
    kpi_lead_neto BIGINT,
    kpi_lead_bruto BIGINT,
    activo BIGINT,
    id_classlife STRING,
    id_tipo_registro BIGINT,
    tipo_registro string,
    id_dim_propietario_lead BIGINT,
    id_dim_programa BIGINT,
    id_dim_producto BIGINT,
    id_dim_utm_campaign BIGINT,
    id_dim_utm_ad BIGINT,
    id_dim_utm_source BIGINT,
    id_dim_nacionalidad BIGINT,
    id_dim_tipo_formacion BIGINT,
    id_dim_tipo_negocio BIGINT,
    id_dim_modalidad BIGINT,
    id_dim_institucion BIGINT,
    id_dim_sede BIGINT,
    id_dim_pais INT,
    id_dim_estado_venta BIGINT,
    id_dim_etapa_venta BIGINT,
    id_dim_motivo_perdida BIGINT,
    id_dim_vertical BIGINT,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/fctventa';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,origenClasslife
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.origenClasslife 
(
    id_Dim_Origen_SIS BIGINT PRIMARY KEY,
    nombre_Origen_SIS STRING NOT NULL,
    api_Client INT NOT NULL,
    codigo_Origen_SIS STRING NOT NULL,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP 
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/origenClasslife';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_estado_matricula
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_estado_matricula 
(
    id_dim_estado_matricula BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1) PRIMARY KEY,
    cod_estado_matricula STRING NOT NULL,
    estado_matricula STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_estado_matricula';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_estudiante
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_estudiante 
(
    id_dim_estudiante BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1) PRIMARY KEY,
    id_origen_sis BIGINT NOT NULL,
    cod_estudiante STRING NOT NULL,
    nombre_estudiante STRING,
    email STRING,
    phone STRING,
    fecha_creacion TIMESTAMP,
    estado STRING,
    edad STRING,
    id_zoho STRING,
    pais STRING,
    ciudad STRING,
    codigo_postal STRING,
    direccion_postal STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_estudiante';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,fct_matricula
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.fct_matricula
(
    id_matricula BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1),
    id_origen_SIS BIGINT,
    cod_matricula STRING,
    id_dim_estudiante BIGINT,
    id_dim_programa BIGINT,
    id_dim_modalidad BIGINT,
    id_dim_institucion BIGINT,
    id_dim_sede BIGINT,
    id_dim_producto BIGINT,
    id_dim_tipo_formacion BIGINT,
    id_dim_tipo_negocio BIGINT,
    id_dim_pais BIGINT,
    ano_curso INT,
    fec_matricula DATE,
    id_dim_estado_matricula BIGINT,
    fec_anulacion TIMESTAMP,
    fec_finalizacion TIMESTAMP,
    nota_media BIGINT,
    cod_descuento STRING,
    importe_matricula DECIMAL(10,2),
    importe_descuento DECIMAL(10,2),
    importe_cobros DECIMAL(10,2),
    tipo_pago STRING,
    edad_acceso STRING,
    zoho_deal_id STRING,
    fec_ultimo_login_LMS TIMESTAMP,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/fct_matricula';
"""

spark.sql(sql_query)


# COMMAND ----------

# DBTITLE 1,dim_concepto_cobro
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_concepto_cobro 
(
    id_dim_concepto_cobro BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1) PRIMARY KEY,
    concepto STRING,
    tipo_reparto STRING,
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_concepto_cobro';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,fct_recibos
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.fct_recibos
(
    id_recibo BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1),
    id_origen_SIS INT,
    cod_recibo STRING,
    cod_matricula STRING,
    id_dim_concepto_cobro BIGINT,
    fecha_emision TIMESTAMP,
    fecha_vencimiento TIMESTAMP,
    fecha_pago TIMESTAMP,
    estado STRING,
    importe_recibo DECIMAL(18,2),
    tiene_factura STRING,
    forma_pago STRING,
    id_dim_estudiante BIGINT,
    id_dim_producto BIGINT,
    id_dim_programa BIGINT,
    id_dim_modalidad BIGINT,
    id_dim_institucion BIGINT,
    id_dim_sede BIGINT,
    id_dim_tipo_formacion BIGINT,
    id_dim_tipo_negocio BIGINT,
    fec_inicio_reconocimiento DATE,
    fec_fin_reconocimiento DATE,
    meses_reconocimiento INT,
    importe_Mensual_Reconocimiento DECIMAL(18,2),
    ETLcreatedDate TIMESTAMP,
    ETLupdatedDate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/fct_recibos';
"""
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,auxiliar_periodificacion
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.auxiliar_periodificacion
(
    fecha_devengo DATE,
    tipo_cargo STRING,
    id_fct_matricula INT,
    id_dim_estudiante INT,
    id_dim_producto INT,
    id_dim_programa INT,
    id_dim_modalidad INT,
    id_dim_institucion INT,
    id_dim_sede INT,
    id_dim_tipo_formacion INT,
    id_dim_tipo_negocio INT,
    fecha_inicio DATE,
    fecha_fin DATE,
    dias_duracion INT,
    modalidad STRING,
    importe DECIMAL(18,2),
    importe_diario DECIMAL(18,2)
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/auxiliar_periodificacion';
"""
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_escenario_presupuesto
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_escenario_presupuesto
(
    id_dim_escenario_presupuesto BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1) PRIMARY KEY
    ,nombre_escenario STRING
    ,etlcreateddate TIMESTAMP
    ,etlupdateddate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_escenario_presupuesto';
"""
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,fct_budget
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.fct_budget
(
     id_fct_budget BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1)
    ,id_dim_fecha_budget DATE
    ,id_dim_escenario_presupuesto INT
    ,id_Dim_Producto INT
    ,escenario STRING
    ,producto STRING
    ,num_Leads_Netos BIGINT
    ,num_Leads_Brutos BIGINT
    ,num_Matriculas BIGINT
    ,importe_matriculacion DECIMAL(10,2)
    ,importe_Captacion DECIMAL(10,2)
    ,id_Dim_Programa INT
    ,id_dim_modalidad INT
    ,id_dim_institucion INT
    ,id_dim_sede INT
    ,id_dim_tipo_formacion INT
    ,id_dim_tipo_negocio INT
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/fct_budget';
"""
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,dim_tipo_conversion
sql_query = f"""
CREATE TABLE IF NOT EXISTS gold_lakehouse.dim_tipo_conversion
(
    id_dim_tipo_conversion BIGINT GENERATED ALWAYS AS IDENTITY (START WITH -1 INCREMENT BY 1) PRIMARY KEY
    ,tipo_conversion STRING
    ,etlcreateddate TIMESTAMP
    ,etlupdateddate TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_tipo_conversion';
"""
spark.sql(sql_query)
