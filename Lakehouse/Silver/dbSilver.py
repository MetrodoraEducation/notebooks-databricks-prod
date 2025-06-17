# Databricks notebook source
# MAGIC %md
# MAGIC **DB Silver**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS silver_lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC **Definición Storage Account**

# COMMAND ----------

storage_account_name = "stmetrodoralakehousepro"

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla Budget**

# COMMAND ----------

# DBTITLE 1,budget

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.budget
(
fecha date,
escenario string,
titulacion string,
centro string,
sede string,
modalidad string,
num_leads_netos integer,
num_leads_brutos integer,
new_enrollment integer,
importe_venta_neta double,
importe_venta_bruta double,
importe_captacion double,
processdate timestamp
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/budget';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **tabla aircallcalls**

# COMMAND ----------

# DBTITLE 1,aircallcalls

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.aircallcalls
(
country_code_a2 string,
direction string,
duration integer,
ended_at timestamp,
id string,
missed_call_reason string,
raw_digits string,
started_at timestamp,
processdate timestamp
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/aircallcalls';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla clientifydeals**

# COMMAND ----------

# DBTITLE 1,clientifydeals

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.clientifydeals
(
id string,
actual_closed_date timestamp,
amount double,
amount_user double,
company string,
company_name string,
contact string,
contact_email string,
contact_medium string,
contact_name string,
contact_phone string,
contact_source string,
created timestamp,
currency string,
deal_source string,
expected_closed_date timestamp,
lost_reason string,
modified timestamp,
name string,
owner string,
owner_name string,
pipeline string,
pipeline_desc string,
pipeline_stage string,
pipeline_stage_desc string,
probability integer,
probability_desc double,
source long,
status integer,
status_desc string,
custom_fields_byratings_rating string,
custom_fields_byratings_score double,
custom_fields_estudio_old string,
custom_fields_id string,
custom_fields_modalidad_old string,
custom_fields_sede_old string,
custom_fields_anio_academico string,
custom_fields_campaign_id string,
custom_fields_centro string,
custom_fields_ciudad string,
custom_fields_cp string,
custom_fields_curso_anio string,
custom_fields_descuento double,
custom_fields_descuento_matricula double,
custom_fields_estudio string,
custom_fields_fecha_inscripcion timestamp,
custom_fields_gclid string,
custom_fields_gdpr string,
custom_fields_google_id string,
custom_fields_linea_negocio string,
custom_fields_matricula double,
custom_fields_mensualidad double,
custom_fields_modalidad string,
custom_fields_pais string,
custom_fields_ref string,
custom_fields_sede string,
custom_fields_tipo_conversion string,
custom_fields_turno string,
custom_fields_ua string,
custom_fields_url string,
custom_fields_utm_ad_id string,
custom_fields_utm_adset_id string,
custom_fields_utm_campaign string,
custom_fields_utm_campaign_id string,
custom_fields_utm_campaign_name string,
custom_fields_utm_channel string,
custom_fields_utm_device string,
custom_fields_utm_estrategia string,
custom_fields_utm_medium string,
custom_fields_utm_network string,
custom_fields_utm_placement string,
custom_fields_utm_site_source_name string,
custom_fields_utm_source string,
custom_fields_utm_term string,
custom_fields_utm_type string,
processdate timestamp,
sourcesystem string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/clientifydeals';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla odoolead**

# COMMAND ----------

# DBTITLE 1,odoolead

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.odoolead
(
id string,
campaign_id string,
city string,
contact_name string,
create_date string,
date_action_last timestamp,
date_closed timestamp,
date_conversion timestamp,
date_last_stage_update timestamp,
email_cc string,
email_from string,
medium_id string,
mobile string,
name string,
partner_name string,
phone string,
planned_revenue double,
probability double,
sale_amount_total double,
source_id string,
street string,
street2 string,
title string,
write_date timestamp,
x_codcurso string,
x_codmodalidad string,
x_curso string,
x_ga_campaign string,
x_ga_medium string,
x_ga_source string,
x_ga_utma string,
x_studio_field_fm3fx string,
zip string,
stage_id string,
stage_value string,
company_id string,
company_value string,
country_id string,
country_value string,
state_id string,
state_value string,
user_id string,
user_value string,
x_curso_id string,
x_curso_value string,
x_modalidad_id string,
x_modalidad_value string,
x_sede_id string,
x_sede_value string,
lost_reason_id string,
lost_reason_value string,
processdate timestamp,
sourcesystem string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/odoolead';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla sales**

# COMMAND ----------

# DBTITLE 1,sales

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.sales
(
cod_venta string,
sistema_origen string,
fec_procesamiento TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/sales';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla clientifydealsidfordelete**

# COMMAND ----------

# DBTITLE 1,clientifydealsidfordelete

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.clientifydealsidfordelete
(
id string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/clientifydealsidfordelete';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dimPais**

# COMMAND ----------

# DBTITLE 1,dim_pais

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.dim_pais
(
    id int,
    nombre string,
    name string,
    nombre_nacionalidad string,
    iso2 string,	
    iso3 string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_pais';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla Mapeo_Origen_Campania**

# COMMAND ----------

# DBTITLE 1,mapeo_origen_campania

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.mapeo_origen_campania
(
utm_source string, 
utm_type string, 
utm_channel string, 
utm_medium string 
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_origen_campania';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla mapeo_modalidad**

# COMMAND ----------

# DBTITLE 1,mapeo_modalidad

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.mapeo_modalidad
(
modalidad string, 
modalidad_norm string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_modalidad';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla mapeo_sede**

# COMMAND ----------

# DBTITLE 1,mapeo_sede

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.mapeo_sede
(
sede string, 
sede_norm string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_sede';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla mapeo_estudio**

# COMMAND ----------

# DBTITLE 1,mapeo_estudio

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.mapeo_estudio
(
    estudio string, 
    estudio_norm string
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/mapeo_estudio';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabla dim_estudio**

# COMMAND ----------

# DBTITLE 1,dim_estudio

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.dim_estudio
(
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
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/dim_estudio';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Classlife**

# COMMAND ----------

# DBTITLE 1,Table ClasslifeTitulaciones
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.classlifetitulaciones
(
     modalidad STRING
    ,fecha_inicio_docencia STRING
    ,fecha_inicio STRING
    ,meses_cursos_open STRING
    ,admisionsino STRING
    ,degree_title STRING
    ,grupo STRING
    ,degree_id STRING
    ,meses_duracion STRING
    ,horas_acreditadas STRING
    ,plazas STRING
    ,fecha_fin_pago STRING
    ,horas_presenciales_2 STRING
    ,num_plazas_ultimas STRING
    ,codigo_programa STRING
    ,area_title STRING
    ,fecha_inicio_cuotas STRING
    ,enroll_group_id STRING
    ,tarifa_ampliacion STRING
    ,tarifa_euneiz STRING
    ,term_title STRING
    ,certificado_euneiz_incluido_2 STRING
    ,especialidad STRING
    ,fecha_fin STRING
    ,creditos STRING
    ,enroll_group_id_2 STRING
    ,counters_pre_enrolled STRING
    ,fecha_fin_cuotas STRING
    ,ano_inicio_docencia STRING
    ,fecha_fin_reconocimiento_ingresos STRING
    ,term_id STRING
    ,fecha_inicio_reconocimiento_ingresos STRING
    ,group_vertical STRING
    ,tarifa_matricula STRING
    ,area_sede STRING
    ,area_id STRING
    ,admisionsino_2 STRING
    ,year STRING
    ,codigo_sede STRING
    ,plan_title STRING
    ,no_ultimas_plazas STRING
    ,zoho_id STRING
    ,ano_inicio_docencia_2 STRING
    ,mesesampliacion STRING
    ,codigo_antiguo STRING
    ,nombre_del_programa_oficial_completo STRING
    ,codigo_entidad_legal STRING
    ,fecha_fin_docencia STRING
    ,nombreweb STRING
    ,area_codigo_vertical STRING
    ,tiponegocio_2 STRING
    ,enroll_end STRING
    ,modalidad_code STRING
    ,area_entidad_legal STRING
    ,ciclo_title STRING
    ,building_title STRING
    ,school_id STRING
    ,grupo_2 STRING
    ,building_id STRING
    ,plan_id STRING
    ,counters_availables STRING
    ,ultima_actualizacion STRING
    ,mes_inicio_docencia_2 STRING
    ,counters_enrolled STRING
    ,horas_acreditadas_2 STRING
    ,receipts_count STRING
    ,tiponegocio STRING
    ,horas_presenciales STRING
    ,enroll_group_name STRING
    ,fecha_inicio_pago STRING
    ,enroll_alias STRING
    ,building STRING
    ,school_name STRING
    ,cuotas_docencia STRING
    ,enroll_ini STRING
    ,acreditado STRING
    ,descripcion_calendario STRING
    ,destinatarios STRING
    ,enroll_pago_ini_t STRING
    ,nombre_antiguo_de_programa STRING
    ,certificado_euneiz_incluido STRING
    ,group_entidad_legal STRING
    ,area_vertical STRING
    ,group_entidad_legal_codigo STRING
    ,counters_seats STRING
    ,codigo_especialidad STRING
    ,descripcion_calendario_2 STRING
    ,ciclo_id STRING
    ,section_id STRING
    ,codigo_vertical STRING
    ,mes_inicio_docencia STRING
    ,section_title STRING
    ,area_entidad_legal_codigo STRING
    ,group_sede STRING
    ,fecha_creacion STRING
    ,tarifa_docencia STRING
    ,total_tarifas STRING
    ,group_codigo_vertical STRING
    ,roaster_ind STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/classlifetitulaciones';
"""

# Ejecutar la consulta SQL con Spark
spark.sql(sql_query)


# COMMAND ----------

# DBTITLE 1,ClasslifeStudents
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.classlifeStudents
(
    centredeprocedencia STRING,
    modalidad STRING,
    profesion STRING,
    fiscaladmit_codipais STRING,
    lead_phone STRING,
    lead_name STRING,
    factura_correo STRING,
    cip STRING,
    lead_source STRING,
    anydepreinscripcio STRING,
    degree_id STRING,
    newsletter STRING,
    foce_create_lead STRING,
    student_language STRING,
    fiscaladmit_direccion STRING,
    language STRING,
    year_id STRING,
    direccion STRING,
    databaixaacademica STRING,
    fiscaladmit_iban STRING,
    codipaisnaixement STRING,
    titulouniversitariofisioterapia STRING,
    lead_alias STRING,
    basephone STRING,
    colegiadoprofesional STRING,
    ciudad STRING,
    seguridadsocial STRING,
    factura_pais STRING,
    lastname STRING,
    datosacceso_ultim_estudi_matriculat STRING,
    certificadouniversitariosolicitudtitulo STRING,
    dnumero STRING,
    codipreinscripcio STRING,
    student_blocked STRING,
    student_phone STRING,
    student_email STRING,
    student_uid STRING,
    descala STRING,
    dplanta STRING,
    fiscaladmit_movil STRING,
    student_active STRING,
    student_lastname STRING,
    email2 STRING,
    ncolegiado STRING,
    term_id STRING,
    phone STRING,
    admit_dni_front STRING,
    classlife_uid STRING,
    dtipus STRING,
    factura_ciudad STRING,
    lead_lastnameend STRING,
    fiscaladmit_cif STRING,
    area_id STRING,
    lead_area STRING,
    enroll_ref STRING,
    lead_email STRING,
    zoho_id STRING,
    excludesecurityarraymetas STRING,
    dpuerta STRING,
    politicas STRING,
    sexo STRING,
    lead_segment STRING,
    student_id STRING,
    datosacceso_curs_ultim_estudi_matriculat STRING,
    lead_lastname STRING,
    lead_status STRING,
    nommunicipinaixementfora STRING,
    school_id STRING,
    fiscaladmit_lastnameend STRING,
    student_lastnameend STRING,
    fiscaladmit_correo STRING,
    dbloc STRING,
    factura_numfiscal STRING,
    lead_count STRING,
    nia STRING,
    factura_nomfactura STRING,
    edad STRING,
    incompany STRING,
    telefono STRING,
    fiscaladmit_codigo STRING,
    comunidadautonoma STRING,
    admit_dni_back STRING,
    codigo STRING,
    lastnameend STRING,
    lead_admission STRING,
    lead_asnew STRING,
    email STRING,
    student_name STRING,
    name STRING,
    pais STRING,
    school_id_2 STRING,
    datosacceso_pais_ultim_curs_matriculat STRING,
    medicas STRING,
    lead_id STRING,
    ciclo_id STRING,
    section_id STRING,
    factura_movil STRING,
    codinacionalitat STRING,
    factura_direccion STRING,
    provincia STRING,
    codiprovincianaixement STRING,
    telefono2 STRING,
    codimunicipinaixement STRING,
    matricula4gradofisioterapia STRING,
    fiscaladmit_ciudad STRING,
    student_key STRING,
    fiscaladmit_titular STRING,
    codipais STRING,
    lead_language STRING,
    student_full_name STRING,
    tipusdocument STRING,
    factura_codigo STRING,
    lead_message_read STRING,
    group STRING,
    nacimiento STRING,
    lead_date STRING,
    created_on STRING,
    updated_at STRING,
    dataingres STRING,
    student_registration_date STRING,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/classlifeStudents';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,ClasslifeEnrollments
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.ClasslifeEnrollments
(
    admisiones STRING,
    codigo_promocion_id STRING,
    enroll_ini STRING,
    modalidad STRING,
    paymentmethod STRING,
    lead_admission STRING,
    lead_segment STRING,
    lead_asnew STRING,
    degree_title STRING,
    lead_date DATE,
    enroll_id STRING,
    student_id STRING,
    lead_message_read STRING,
    lead_phone STRING,
    lead_lastname STRING,
    lead_status STRING,
    lead_name STRING,
    totalenroll STRING,
    enroll_end STRING,
    lead_source STRING,
    paymentmethodwannme STRING,
    degree_id STRING,
    newsletter STRING,
    school_id_2 STRING,
    codigo_promocion STRING,
    created_on TIMESTAMP,
    term_id STRING,
    enroll_group STRING,
    ciclo_title STRING,
    enroll_stage STRING,
    school_id STRING,
    lead_id STRING,
    lead_lastnameend STRING,
    admisiones_acepta_candidato STRING,
    tipopagador STRING,
    ciclo_id STRING,
    section_id STRING,
    area_id STRING,
    lead_area STRING,
    acceso_euneiz STRING,
    lead_email STRING,
    enroll_alias STRING,
    year STRING,
    section_title STRING,
    enroll_in STRING,
    lead_count INT,
    updated_at TIMESTAMP,
    lead_alias STRING,
    suma_descuentos DOUBLE,
    area_title STRING,
    incompany STRING,
    enroll_step STRING,
    student_full_name STRING,
    lead_language STRING,
    enroll_status_id STRING,
    enroll_status STRING,
    excludesecurityarraymetas STRING,
    updated_at_2 TIMESTAMP,
    term_title STRING,
    school_name STRING,
    zoho_deal_id STRING,
    first_activate_enroll STRING,
    fee_title_docencia STRING,
    fee_title_matricula STRING,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/ClasslifeEnrollments';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,ClasslifeReceipts
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.ClasslifeReceipts
(
    receipt_id STRING,
    receipt_tax_per DOUBLE,
    payment_method STRING,
    receipt_tax DOUBLE,
    student_id STRING,
    enroll_id STRING,
    remittance_id STRING,
    receipt_total DOUBLE,
    invoice_id STRING,
    receipt_concept STRING,
    receipt_status_id STRING,
    student_full_name STRING,
    receipt_price DOUBLE,
    receipt_status STRING,
    payment_method_id STRING,
    receipt_advanced DOUBLE,
    emission_date TIMESTAMP,
    expiry_date TIMESTAMP,
    collection_date TIMESTAMP,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/ClasslifeReceipts';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,ClasslifeAdmissions
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.ClasslifeAdmissions
(
    student_phone STRING,
    comercial STRING,
    student_email STRING,
    ini_date STRING,
    zoho_deal_id STRING,
    enroll_group STRING,
    ciclo_title STRING,
    id STRING,
    student_dni STRING,
    registration_date STRING,
    year_id STRING,
    student_full_name STRING,
    area_title STRING,
    school_name STRING,
    end_date STRING,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/ClasslifeAdmissions';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Zoho CRM**

# COMMAND ----------

# DBTITLE 1,Table ZohoLeads
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.zoholeads
(
        id STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        mobile STRING,
        modified_time TIMESTAMP,
        lead_source STRING,
        lead_status STRING,
        lead_rating STRING,
        lead_scoring STRING,
        visitor_score STRING,
        sexo STRING,
        tipologia_cliente STRING,
        tipo_conversion STRING,
        residencia STRING,
        provincia STRING,
        motivos_perdida STRING,
        nacionalidad STRING,
        utm_source STRING,
        utm_medium STRING,
        utm_campaign_id STRING,
        utm_campaign_name STRING,
        utm_ad_id STRING,
        utm_adset_id STRING,
        utm_term STRING,
        utm_channel STRING,
        utm_type STRING,
        utm_strategy STRING,
        utm_profile STRING,
        google_click_id STRING,
        facebook_click_id STRING,
        id_producto STRING,
        id_programa STRING,
        apellido_2 STRING,
        lead_correlation_id STRING,
        description STRING,
        phone STRING,
        device STRING,
        source STRING,
        owner_email STRING,
        owner_id STRING,
        owner_name STRING,
        linea_de_negocio STRING,
        Created_Time TIMESTAMP,
        processdate TIMESTAMP,
        sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/zoholeads';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Table ZohoDeals

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.zohodeals
(
        id STRING,
        importe DOUBLE,
        codigo_descuento STRING,
        fecha_cierre DATE,
        competencia STRING,
        currency STRING,
        deal_name STRING,
        descuento DOUBLE,
        exchange_rate DOUBLE,
        fecha_hora_anulacion TIMESTAMP,
        fecha_hora_documentacion_completada TIMESTAMP,
        fecha_hora_pagado TIMESTAMP,
        id_classlife STRING,
        id_lead STRING,
        id_producto STRING,
        importe_pagado DOUBLE,
        modified_time TIMESTAMP,
        motivo_perdida_b2b STRING,
        motivo_perdida_b2c STRING,
        pipeline STRING,
        probabilidad INT,
        profesion_estudiante STRING,
        residencia1 STRING,
        etapa STRING,
        tipologia_cliente STRING,
        br_rating STRING,
        br_score DOUBLE,
        network STRING,
        tipo_conversion STRING,
        utm_ad_id STRING,
        utm_adset_id STRING,
        utm_campana_id STRING,
        utm_campana_nombre STRING,
        utm_canal STRING,
        utm_estrategia STRING,
        utm_medio STRING,
        utm_perfil STRING,
        utm_fuente STRING,
        utm_termino STRING,
        utm_tipo STRING,
        processdate TIMESTAMP,
        sourcesystem STRING,
        tipo_cambio DOUBLE,
        utm_campaign_id STRING,
        utm_campaign_name STRING,
        utm_channel STRING,
        utm_strategy STRING,
        utm_medium STRING,
        utm_profile STRING,
        utm_source STRING,
        utm_term STRING,
        utm_type STRING,
        owner_email STRING,
        owner_id STRING,
        owner_name STRING,
        nacionalidad1 STRING,
        lead_correlation_id STRING,
        id_unico STRING,
        Created_Time TIMESTAMP,
        Tipologia_alumno1 STRING,
        Contact_Name_id STRING,
        linea_de_negocio STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/zohodeals';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Table ZohoUsers
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.zohousers
(
    next_shift STRING,
    shift_effective_from TIMESTAMP,
    currency STRING,
    isonline BOOLEAN,
    modified_time TIMESTAMP,
    alias STRING,
    city STRING,
    confirm BOOLEAN,
    country STRING,
    country_locale STRING,
    created_time TIMESTAMP,
    date_format STRING,
    decimal_separator STRING,
    default_tab_group STRING,
    dob DATE,
    email STRING,
    fax STRING,
    first_name STRING,
    full_name STRING,
    id STRING,
    language STRING,
    last_name STRING,
    locale STRING,
    microsoft BOOLEAN,
    mobile STRING,
    number_separator STRING,
    offset BIGINT,
    personal_account BOOLEAN,
    phone STRING,
    sandboxdeveloper BOOLEAN,
    signature STRING,
    state STRING,
    status STRING,
    street STRING,
    time_format STRING,
    time_zone STRING,
    website STRING,
    zip STRING,
    zuid STRING,
    modified_by_id STRING,
    modified_by_name STRING,
    created_by_id STRING,
    created_by_name STRING,
    profile_id STRING,
    profile_name STRING,
    role_id STRING,
    role_name STRING,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/zohousers';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Table ZohoContacts

sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.zohocontacts
(
last_name STRING,
dni STRING,
date_birth STRING,
email STRING,
estudios STRING,
first_name STRING,
home_phone STRING,
id_classlife STRING,
mailing_city STRING,
mailing_country STRING,
mailing_street STRING,
mailing_zip STRING,
mobile STRING,
nacionalidad STRING,
other_city STRING,
other_country STRING,
other_state STRING,
other_street STRING,
other_zip STRING,
phone STRING,
profesion STRING,
provincia STRING,
residencia STRING,
secondary_email STRING,
sexo STRING,
tipo_cliente STRING,
tipo_contacto STRING,
id STRING,
recibir_comunicacion STRING,
ultima_linea_de_negocio STRING,
woztellplatform_whatsapp_out BOOLEAN,
processdate TIMESTAMP,
sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/zohocontacts';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,Table ZohoCampaigns
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.zohocampaigns
(
    actual_cost STRING,
    budgeted_cost STRING,
    campaign_name STRING,
    currency STRING,
    description STRING,
    start_date STRING,
    end_date STRING,
    exchange_rate STRING,
    expected_response STRING,
    expected_revenue STRING,
    business_line STRING,
    campaign_subject STRING,
    reply_to_address STRING,
    sender_address STRING,
    sender_name STRING,
    departmen_tid STRING,
    survey STRING,
    survey_department STRING,
    survey_type STRING,
    survey_url STRING,
    webinar_duration STRING,
    webinar_launch_url STRING,
    webinar_registration_url STRING,
    webinar_schedule STRING,
    num_sent STRING,
    parent_campaign STRING,
    status STRING,
    type STRING,
    id STRING,
    created_by_email STRING,
    created_by_id STRING,
    created_by_name STRING,
    layout_id STRING,
    layout_name STRING,
    modified_by_email STRING,
    modified_by_id STRING,
    modified_by_name STRING,
    owner_email STRING,
    owner_id STRING,
    owner_name STRING,
    tag_color_code STRING,
    tag_id STRING,
    tag_name STRING,
    processDate TIMESTAMP,
    sourceSystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/zohocampaigns';
"""

# Ejecutar la consulta SQL con Spark
spark.sql(sql_query)


# COMMAND ----------

# DBTITLE 1,Tablon Leads_and_Deals
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.tablon_leads_and_deals
(
    id_tipo_registro BIGINT,
    tipo_registro STRING,
    cod_Lead STRING,
    lead_Nombre STRING,
    Nombre STRING,
    Apellido1 STRING,
    Apellido2 STRING,
    email STRING,
    telefono1 STRING,
    nacionalidad STRING,
    telefono2 STRING,
    provincia STRING,
    residencia STRING,
    sexo STRING,
    linea_de_negocio STRING,
    lead_Rating STRING,
    leadScoring DOUBLE,
    etapa STRING,
    motivo_Perdida STRING,
    probabilidad_Conversion DOUBLE,
    flujo_Venta STRING,
    profesion_Estudiante STRING,
    competencia STRING,
    tipo_Cliente_lead STRING,
    tipo_conversion_lead STRING,
    utm_ad_id STRING,
    utm_adset_id STRING,
    utm_campaign_id STRING,
    utm_campaign_name STRING,
    utm_channel STRING,
    utm_estrategia STRING,
    utm_medium STRING,
    utm_perfil STRING,
    utm_source STRING,
    utm_term STRING,
    utm_type STRING,
    cod_Owner STRING,
    cod_Producto STRING,
    lead_Correlation STRING,
    fecha_Creacion_Lead TIMESTAMP,
    fecha_Modificacion_Lead TIMESTAMP,
    cod_Oportunidad STRING,
    cod_Classlife STRING,
    nombre_Oportunidad STRING,
    cod_Contacto STRING,
    fecha_Cierre TIMESTAMP,
    cod_Unico_Zoho STRING,
    ratio_Moneda DOUBLE,
    moneda STRING,
    importe_Pagado DOUBLE,
    cod_Descuento STRING,
    pct_Descuento DOUBLE,
    importe DOUBLE,
    tipo_Alumno STRING,
    tipo_Conversion_opotunidad STRING,
    tipo_Cliente_oportunidad STRING,
    id_classlife STRING,
    fecha_hora_Pagado TIMESTAMP,
    fecha_Creacion_Oportunidad TIMESTAMP,
    fecha_Modificacion_Oportunidad TIMESTAMP,
    nombre_estado_venta STRING,
    fecha_hora_anulacion TIMESTAMP,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/tablon_leads_and_deals';
"""

# Ejecutar la consulta SQL con Spark
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,budget_ff
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.budget_ff
(
     fecha DATE
    ,escenario STRING
    ,producto STRING
    ,num_Leads_Netos STRING
    ,num_Leads_Brutos STRING
    ,num_Matriculas STRING
    ,importe_Venta_Neta STRING
    ,importe_Captacion STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/budget_ff';
"""

# Ejecutar la consulta SQL con Spark
spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **CLASSLIFE_931**

# COMMAND ----------

# DBTITLE 1,ClasslifeAdmissions_931
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.ClasslifeAdmissions_931 (
    student_phone STRING,
    comercial STRING,
    student_email STRING,
    ini_date STRING,
    zoho_deal_id STRING,
    enroll_group STRING,
    ciclo_title STRING,
    id STRING,
    student_dni STRING,
    registration_date STRING,
    year_id STRING,
    student_full_name STRING,
    area_title STRING,
    school_name STRING,
    end_date STRING,
    suma_docencia STRING,
    total_Enroll STRING,
    total_Final STRING,
    total_FinalDocencia STRING,
    total_FinalMatricula STRING,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/ClasslifeAdmissions_931';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,ClasslifeEnrollments_931
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.ClasslifeEnrollments_931
(
     enroll_id STRING
    ,area_id STRING
    ,area_title STRING
    ,ciclo_id STRING
    ,ciclo_title STRING
    ,created_on STRING
    ,degree_id STRING
    ,degree_title STRING
    ,enroll_alias STRING
    ,enroll_end STRING
    ,enroll_group STRING
    ,enroll_in STRING
    ,enroll_ini STRING
    ,enroll_status STRING
    ,enroll_status_id STRING
    ,first_activate_enroll STRING
    ,school_id STRING
    ,school_name STRING
    ,section_id STRING
    ,section_title STRING
    ,student_full_name STRING
    ,student_id STRING
    ,term_title STRING
    ,updated_at STRING
    ,year STRING
    ,zoho_deal_id STRING
    ,cuota_matricula STRING
    ,importe_matricula DECIMAL(18,2)
    ,importe_docencia DECIMAL(18,2)
    ,suma_descuentos DECIMAL(18,2)
    ,total_fees DECIMAL(18,2)
    ,total_restantes DECIMAL(18,2)
    ,processdate TIMESTAMP
    ,sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/ClasslifeEnrollments_931';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,classlifetitulaciones_931
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.classlifetitulaciones_931
(
    admisionsino STRING,
    degree_title STRING,
    horas_acreditadas STRING,
    plazas STRING,
    codigo_programa STRING,
    area_title STRING,
    fecha_inicio_cuotas STRING,
    enroll_group_id STRING,
    tarifa_ampliacion STRING,
    tarifa_euneiz STRING,
    term_title STRING,
    especialidad STRING,
    fecha_fin STRING,
    enroll_group_id_2 STRING,
    fecha_fin_cuotas STRING,
    ano_inicio_docencia STRING,
    fecha_fin_reconocimiento_ingresos STRING,
    term_id STRING,
    fecha_inicio_reconocimiento_ingresos STRING,
    group_vertical STRING,
    tarifa_matricula STRING,
    zoho_id STRING,
    nombre_del_programa_oficial_completo STRING,
    codigo_entidad_legal STRING,
    fecha_fin_docencia STRING,
    nombreweb STRING,
    area_codigo_vertical STRING,
    enroll_end STRING,
    area_entidad_legal STRING,
    ciclo_title STRING,
    school_id STRING,
    ultima_actualizacion STRING,
    horas_acreditadas_2 STRING,
    tiponegocio STRING,
    enroll_group_name STRING,
    enroll_alias STRING,
    school_name STRING,
    nombre_antiguo_de_programa STRING,
    group_entidad_legal STRING,
    area_vertical STRING,
    section_id STRING,
    section_title STRING,
    area_entidad_legal_codigo STRING,
    group_sede STRING,
    fecha_creacion STRING,
    tarifa_docencia STRING,
    total_tarifas STRING,
    group_codigo_vertical STRING,
    fecha_inicio_docencia STRING,
    fecha_inicio STRING,
    grupo STRING,
    meses_duracion STRING,
    fecha_fin_pago STRING,
    creditos STRING,
    year STRING,
    codigo_sede STRING,
    modalidad_code STRING,
    horas_presenciales STRING,
    fecha_inicio_pago STRING,
    cuotas_docencia STRING,
    codigo_especialidad STRING,
    ciclo_id STRING,
    acreditado STRING,
    codigo_vertical STRING,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/classlifetitulaciones_931';
"""

# Ejecutar creación de tabla
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,ClasslifeReceipts_931
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.ClasslifeReceipts_931 (
    receipt_id STRING,
    receipt_tax_per STRING,
    payment_method STRING,
    receipt_tax STRING,
    student_id STRING,
    enroll_id STRING,
    remittance_id STRING,
    receipt_total STRING,
    invoice_id STRING,
    emission_date STRING,
    expiry_date STRING,
    receipt_status STRING,
    payment_method_id STRING,
    receipt_advanced STRING,
    collection_date TIMESTAMP,
    receipt_concept STRING,
    receipt_status_id STRING,
    student_full_name STRING,
    receipt_price STRING,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/ClasslifeReceipts_931';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,ClasslifeStudents_931
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.ClasslifeStudents_931 (
    student_id STRING,
    student_active STRING,
    student_email STRING,
    student_full_name STRING,
    ciudad STRING,
    codigo STRING,
    direccion STRING,
    edad STRING,
    pais STRING,
    student_phone STRING,
    student_registration_date STRING,
    zoho_id STRING,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/ClasslifeStudents_931';
"""

spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **ZOHO_38B**

# COMMAND ----------

# DBTITLE 1,zohodeals_38b
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.zohodeals_38b (
    id STRING,
    id_producto STRING,
    importe STRING,
    codigo_descuento STRING,
    fecha_cierre STRING,
    competencia STRING,
    fecha_creacion STRING,
    nombre_oportunidad STRING,
    descuento STRING,
    fecha_hora_anulacion STRING,
    fecha_hora_documentacion_completada STRING,
    fecha_hora_pagado STRING,
    id_classlife STRING,
    id_lead STRING,
    importe_pagado STRING,
    linea_de_negocio STRING,
    fecha_modificacion STRING,
    motivo_perdida_b2b STRING,
    motivo_perdida_b2c STRING,
    nacionalidad STRING,
    flujo_venta STRING,
    probabilidad_conversion STRING,
    profesion_estudiante STRING,
    residencia STRING,
    etapa STRING,
    tipologia_cliente STRING,
    tipologia_alumno STRING,
    rating STRING,
    scoring STRING,
    id_unico STRING,
    lead_correlation_id STRING,
    network STRING,
    tipo_conversion STRING,
    utm_ad_id STRING,
    utm_adset_id STRING,
    utm_campaign_id STRING,
    utm_campaign_name STRING,
    utm_channel STRING,
    utm_estrategia STRING,
    utm_medium STRING,
    utm_perfil STRING,
    utm_source STRING,
    utm_term STRING,
    utm_type STRING,
    contact_name_id STRING,
    contact_name STRING,
    owner_email STRING,
    owner_id STRING,
    owner_name STRING,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/zohodeals_38b';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,ZohoLeads_38b
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.ZohoLeads_38b (
    id STRING,
    apellido_2 STRING,
    fecha_creacion STRING,
    descripcion STRING,
    email STRING,
    nombre STRING,
    linea_de_negocio STRING,
    apellido_1 STRING,
    lead_source STRING,
    lead_status STRING,
    telefono_movil STRING,
    fecha_modificacion STRING,
    motivos_perdida STRING,
    nacionalidad STRING,
    telefono STRING,
    provincia STRING,
    residencia STRING,
    sexo STRING,
    tipologia_cliente STRING,
    tipo_conversion STRING,
    dispositivo STRING,
    id_producto STRING,
    id_programa STRING,
    lead_correlation_id STRING,
    lead_rating STRING,
    lead_scoring STRING,
    source STRING,
    utm_ad_id STRING,
    utm_adset_id STRING,
    utm_campaign_id STRING,
    utm_campaign_name STRING,
    utm_channel STRING,
    utm_estrategia STRING,
    utm_medium STRING,
    utm_perfil STRING,
    utm_source STRING,
    utm_term STRING,
    utm_type STRING,
    owner_email STRING,
    owner_id STRING,
    owner_name STRING,
    id_oportunidad_asociada STRING,
    name_oportunidad_asociada STRING,
    id_clientify STRING,
    modalidad_curso STRING,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/ZohoLeads_38b';
"""

# Ejecutar en Spark
spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,ZohoUsers_38b
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.ZohoUsers_38b (
    id STRING,
    status STRING,
    isonline STRING,
    linea_de_negocio STRING,
    modified_time TIMESTAMP,
    city STRING,
    confirm STRING,
    country STRING,
    country_locale STRING,
    created_time TIMESTAMP,
    email STRING,
    first_name STRING,
    full_name STRING,
    language STRING,
    last_name STRING,
    mobile STRING,
    modified_by_id STRING,
    modified_by_name STRING,
    created_by_id STRING,
    created_by_name STRING,
    role_id STRING,
    role_name STRING,
    processdate TIMESTAMP,
    sourcesystem STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/ZohoUsers_38b';
"""

spark.sql(sql_query)

# COMMAND ----------

# DBTITLE 1,ZohoContacts_38b
sql_query = f"""
CREATE TABLE IF NOT EXISTS silver_lakehouse.ZohoContacts_38b (
    last_name STRING,
    dni STRING,
    date_birth STRING,
    email STRING,
    first_name STRING,
    id_classlife STRING,
    mailing_city STRING,
    mailing_street STRING,
    mailing_zip STRING,
    mobile STRING,
    modified_time STRING,
    nacionalidad STRING,
    other_city STRING,
    other_country STRING,
    other_state STRING,
    other_street STRING,
    other_zip STRING,
    phone STRING,
    residencia STRING,
    sexo STRING,
    tipo_cliente STRING,
    tipo_contacto STRING,
    id STRING,
    ultima_linea_de_negocio STRING,
    sourcesystem STRING,
    processdate STRING
)
USING DELTA
LOCATION 'abfss://silver@{storage_account_name}.dfs.core.windows.net/lakehouse/ZohoContacts_38b';
"""

spark.sql(sql_query)
