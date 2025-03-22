# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration" 

# COMMAND ----------

# DBTITLE 1,Insert fct_venta lakehouse postgresql
from pyspark.sql.functions import col
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `fct_venta`
def upsert_fct_venta(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión desde la función del Bloque 1
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO fct_venta (
            id_venta,
            cod_venta,
            nombre,
            email,
            telefono,
            nombre_contacto,
            id_dim_propietario_lead,
            id_dim_origen_campania,
            id_dim_campania,
            importe_venta,
            importe_descuento,
            importe_venta_neta,
            id_dim_estado_venta,
            id_dim_etapa_venta,
            posibilidad_venta,
            fec_creacion,
            fec_modificacion,
            fec_cierre,
            id_dim_modalidad,
            id_dim_institucion,
            id_dim_sede,
            id_dim_pais,
            id_dim_estudio,
            fec_pago_matricula,
            importe_matricula,
            importe_descuento_matricula,
            importe_neto_matricula,
            id_dim_localidad,
            id_dim_tipo_formacion,
            id_dim_tipo_negocio,
            nombre_scoring,
            puntos_scoring,
            dias_cierre,
            id_dim_motivo_cierre,
            fec_procesamiento,
            sistema_origen,
            tiempo_de_maduracion,
            new_enrollent,
            lead_neto,
            activo
        )
        VALUES %s
        ON CONFLICT (id_venta) DO UPDATE SET
            cod_venta = EXCLUDED.cod_venta,
            nombre = EXCLUDED.nombre,
            email = EXCLUDED.email,
            telefono = EXCLUDED.telefono,
            nombre_contacto = EXCLUDED.nombre_contacto,
            id_dim_propietario_lead = EXCLUDED.id_dim_propietario_lead,
            id_dim_origen_campania = EXCLUDED.id_dim_origen_campania,
            id_dim_campania = EXCLUDED.id_dim_campania,
            importe_venta = EXCLUDED.importe_venta,
            importe_descuento = EXCLUDED.importe_descuento,
            importe_venta_neta = EXCLUDED.importe_venta_neta,
            id_dim_estado_venta = EXCLUDED.id_dim_estado_venta,
            id_dim_etapa_venta = EXCLUDED.id_dim_etapa_venta,
            posibilidad_venta = EXCLUDED.posibilidad_venta,
            fec_creacion = EXCLUDED.fec_creacion,
            fec_modificacion = EXCLUDED.fec_modificacion,
            fec_cierre = EXCLUDED.fec_cierre,
            id_dim_modalidad = EXCLUDED.id_dim_modalidad,
            id_dim_institucion = EXCLUDED.id_dim_institucion,
            id_dim_sede = EXCLUDED.id_dim_sede,
            id_dim_pais = EXCLUDED.id_dim_pais,
            id_dim_estudio = EXCLUDED.id_dim_estudio,
            fec_pago_matricula = EXCLUDED.fec_pago_matricula,
            importe_matricula = EXCLUDED.importe_matricula,
            importe_descuento_matricula = EXCLUDED.importe_descuento_matricula,
            importe_neto_matricula = EXCLUDED.importe_neto_matricula,
            id_dim_localidad = EXCLUDED.id_dim_localidad,
            id_dim_tipo_formacion = EXCLUDED.id_dim_tipo_formacion,
            id_dim_tipo_negocio = EXCLUDED.id_dim_tipo_negocio,
            nombre_scoring = EXCLUDED.nombre_scoring,
            puntos_scoring = EXCLUDED.puntos_scoring,
            dias_cierre = EXCLUDED.dias_cierre,
            id_dim_motivo_cierre = EXCLUDED.id_dim_motivo_cierre,
            fec_procesamiento = EXCLUDED.fec_procesamiento,
            sistema_origen = EXCLUDED.sistema_origen,
            tiempo_de_maduracion = EXCLUDED.tiempo_de_maduracion,
            new_enrollent = EXCLUDED.new_enrollent,
            lead_neto = EXCLUDED.lead_neto,
            activo = EXCLUDED.activo;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_venta"],
            row["cod_venta"],
            row["nombre"],
            row["email"],
            row["telefono"],
            row["nombre_contacto"],
            row["id_dim_propietario_lead"],
            row["id_dim_origen_campania"],
            row["id_dim_campania"],
            row["importe_venta"],
            row["importe_descuento"],
            row["importe_venta_neta"],
            row["id_dim_estado_venta"],
            row["id_dim_etapa_venta"],
            row["posibilidad_venta"],
            row["fec_creacion"],
            row["fec_modificacion"],
            row["fec_cierre"],
            row["id_dim_modalidad"],
            row["id_dim_institucion"],
            row["id_dim_sede"],
            row["id_dim_pais"],
            row["id_dim_estudio"],
            row["fec_pago_matricula"],
            row["importe_matricula"],
            row["importe_descuento_matricula"],
            row["importe_neto_matricula"],
            row["id_dim_localidad"],
            row["id_dim_tipo_formacion"],
            row["id_dim_tipo_negocio"],
            row["nombre_scoring"],
            row["puntos_scoring"],
            row["dias_cierre"],
            row["id_dim_motivo_cierre"],
            row["fec_procesamiento"],
            row["sistema_origen"],
            row["tiempo_de_maduracion"],
            row["new_enrollent"],
            row["lead_neto"],
            row["activo"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización
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

# Leer datos desde la tabla `gold_lakehouse.fct_venta` en Databricks
source_table = (spark.table("gold_lakehouse.fct_venta")
                .filter(col("id_venta") != -1)
                .select("id_venta", "cod_venta", "nombre", "email", "telefono", "nombre_contacto",
                        "id_dim_propietario_lead", "id_dim_origen_campania", "id_dim_campania",
                        "importe_venta", "importe_descuento", "importe_venta_neta",
                        "id_dim_estado_venta", "id_dim_etapa_venta", "posibilidad_venta",
                        "fec_creacion", "fec_modificacion", "fec_cierre",
                        "id_dim_modalidad", "id_dim_institucion", "id_dim_sede", "id_dim_pais",
                        "id_dim_estudio", "fec_pago_matricula", "importe_matricula",
                        "importe_descuento_matricula", "importe_neto_matricula",
                        "id_dim_localidad", "id_dim_tipo_formacion", "id_dim_tipo_negocio",
                        "nombre_scoring", "puntos_scoring", "dias_cierre", "id_dim_motivo_cierre",
                        "fec_procesamiento", "sistema_origen", "tiempo_de_maduracion",
                        "new_enrollent", "lead_neto", "activo"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_fct_venta)
    logger.info("Proceso completado con éxito (Upsert en fct_venta de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

