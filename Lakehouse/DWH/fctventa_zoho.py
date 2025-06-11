# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert fctventa_zoho lakehouse postgresql
from pyspark.sql.functions import col
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `fctventa`
def upsert_fctventa(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO fctventa (
            id_venta, cod_lead, cod_oportunidad, nombre, email, telefono, nombre_contacto, 
            importe_venta, importe_descuento, importe_venta_neto, posibilidad_venta, ciudad, provincia, calle, 
            codigo_postal, nombre_scoring, puntos_scoring, dias_cierre, fec_creacion, fec_modificacion, 
            fec_cierre, fec_pago_matricula, fecha_hora_anulacion, fecha_Modificacion_Lead, fecha_Modificacion_Oportunidad, 
            importe_matricula, importe_descuento_matricula, importe_neto_matricula, kpi_new_enrollent, 
            kpi_lead_neto, kpi_lead_bruto, activo, id_classlife, id_tipo_registro, tipo_registro, id_dim_propietario_lead, 
            id_dim_programa, id_dim_producto, id_dim_utm_campaign, id_dim_utm_ad, id_dim_utm_source, 
            id_dim_nacionalidad, id_dim_tipo_formacion, id_dim_tipo_negocio, id_dim_modalidad, id_dim_institucion, 
            id_dim_sede, id_dim_pais, id_dim_estado_venta, id_dim_etapa_venta, id_dim_motivo_perdida, id_dim_vertical,
            ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_venta) DO UPDATE SET
            cod_lead = EXCLUDED.cod_lead,
            cod_oportunidad = EXCLUDED.cod_oportunidad,
            nombre = EXCLUDED.nombre,
            email = EXCLUDED.email,
            telefono = EXCLUDED.telefono,
            nombre_contacto = EXCLUDED.nombre_contacto,
            importe_venta = EXCLUDED.importe_venta,
            importe_descuento = EXCLUDED.importe_descuento,
            importe_venta_neto = EXCLUDED.importe_venta_neto,
            posibilidad_venta = EXCLUDED.posibilidad_venta,
            ciudad = EXCLUDED.ciudad,
            provincia = EXCLUDED.provincia,
            calle = EXCLUDED.calle,
            codigo_postal = EXCLUDED.codigo_postal,
            nombre_scoring = EXCLUDED.nombre_scoring,
            puntos_scoring = EXCLUDED.puntos_scoring,
            dias_cierre = EXCLUDED.dias_cierre,
            fec_creacion = EXCLUDED.fec_creacion,
            fec_modificacion = EXCLUDED.fec_modificacion,
            fec_cierre = EXCLUDED.fec_cierre,
            fec_pago_matricula = EXCLUDED.fec_pago_matricula,
            fecha_hora_anulacion = EXCLUDED.fecha_hora_anulacion,
            fecha_Modificacion_Lead = EXCLUDED.fecha_Modificacion_Lead,
            fecha_Modificacion_Oportunidad = EXCLUDED.fecha_Modificacion_Oportunidad,
            importe_matricula = EXCLUDED.importe_matricula,
            importe_descuento_matricula = EXCLUDED.importe_descuento_matricula,
            importe_neto_matricula = EXCLUDED.importe_neto_matricula,
            kpi_new_enrollent = EXCLUDED.kpi_new_enrollent,
            kpi_lead_neto = EXCLUDED.kpi_lead_neto,
            kpi_lead_bruto = EXCLUDED.kpi_lead_bruto,
            activo = EXCLUDED.activo,
            id_classlife = EXCLUDED.id_classlife,
            id_tipo_registro = EXCLUDED.id_tipo_registro,
            tipo_registro = EXCLUDED.tipo_registro,
            id_dim_propietario_lead = EXCLUDED.id_dim_propietario_lead,
            id_dim_programa = EXCLUDED.id_dim_programa,
            id_dim_producto = EXCLUDED.id_dim_producto,
            id_dim_utm_campaign = EXCLUDED.id_dim_utm_campaign,
            id_dim_utm_ad = EXCLUDED.id_dim_utm_ad,
            id_dim_utm_source = EXCLUDED.id_dim_utm_source,
            id_dim_nacionalidad = EXCLUDED.id_dim_nacionalidad,
            id_dim_tipo_formacion = EXCLUDED.id_dim_tipo_formacion,
            id_dim_tipo_negocio = EXCLUDED.id_dim_tipo_negocio,
            id_dim_modalidad = EXCLUDED.id_dim_modalidad,
            id_dim_institucion = EXCLUDED.id_dim_institucion,
            id_dim_sede = EXCLUDED.id_dim_sede,
            id_dim_pais = EXCLUDED.id_dim_pais,
            id_dim_estado_venta = EXCLUDED.id_dim_estado_venta,
            id_dim_etapa_venta = EXCLUDED.id_dim_etapa_venta,
            id_dim_motivo_perdida = EXCLUDED.id_dim_motivo_perdida,
            id_dim_vertical = EXCLUDED.id_dim_vertical,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_venta"], row["cod_lead"], row["cod_oportunidad"], row["nombre"], row["email"], row["telefono"], 
            row["nombre_contacto"], row["importe_venta"], row["importe_descuento"], row["importe_venta_neto"], 
            row["posibilidad_venta"], row["ciudad"], row["provincia"], row["calle"], row["codigo_postal"], 
            row["nombre_scoring"], row["puntos_scoring"], row["dias_cierre"], row["fec_creacion"], 
            row["fec_modificacion"], row["fec_cierre"], row["fec_pago_matricula"], row["fecha_hora_anulacion"], 
            row["fecha_Modificacion_Lead"], row["fecha_Modificacion_Oportunidad"], row["importe_matricula"], 
            row["importe_descuento_matricula"], row["importe_neto_matricula"], row["kpi_new_enrollent"], 
            row["kpi_lead_neto"], row["kpi_lead_bruto"], row["activo"], row["id_classlife"], row["id_tipo_registro"], row["tipo_registro"], 
            row["id_dim_propietario_lead"], row["id_dim_programa"], row["id_dim_producto"], row["id_dim_utm_campaign"], 
            row["id_dim_utm_ad"], row["id_dim_utm_source"], row["id_dim_nacionalidad"], row["id_dim_tipo_formacion"], 
            row["id_dim_tipo_negocio"], row["id_dim_modalidad"], row["id_dim_institucion"], row["id_dim_sede"], 
            row["id_dim_pais"], row["id_dim_estado_venta"], row["id_dim_etapa_venta"], row["id_dim_motivo_perdida"], row["id_dim_vertical"],
            row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            execute_values(cursor, query, values)
            conn.commit()
            logger.info(f"Procesados {len(values)} registros en PostgreSQL (insertados o actualizados).")
        else:
            logger.info("No se encontraron datos válidos en esta partición.")

        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error al procesar registros: {e}")
        raise

# Leer datos desde la tabla `gold_lakehouse.fctventa` en Databricks
source_table = (spark.table("gold_lakehouse.fctventa")
                .select(*["id_venta", "cod_lead", "cod_oportunidad", "nombre", "email", "telefono", "nombre_contacto", "importe_venta", "importe_descuento", "importe_venta_neto", "posibilidad_venta", "ciudad", "provincia", "calle", "codigo_postal", "nombre_scoring", "puntos_scoring", "dias_cierre", "fec_creacion", "fec_modificacion", "fec_cierre", "fec_pago_matricula", "fecha_hora_anulacion", "fecha_Modificacion_Lead", "fecha_Modificacion_Oportunidad", "importe_matricula", "importe_descuento_matricula", "importe_neto_matricula", "kpi_new_enrollent", "kpi_lead_neto", "kpi_lead_bruto", "activo", "id_classlife", "id_tipo_registro", "tipo_registro", "id_dim_propietario_lead", "id_dim_programa", "id_dim_producto", "id_dim_utm_campaign", "id_dim_utm_ad", "id_dim_utm_source", "id_dim_nacionalidad", "id_dim_tipo_formacion", "id_dim_tipo_negocio", "id_dim_modalidad", "id_dim_institucion", "id_dim_sede", "id_dim_pais", "id_dim_estado_venta", "id_dim_etapa_venta", 
                "id_dim_motivo_perdida", "id_dim_vertical", "ETLcreatedDate", "ETLupdatedDate"]))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_fctventa)
    logger.info("Proceso completado con éxito (Upsert en fctventa de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

