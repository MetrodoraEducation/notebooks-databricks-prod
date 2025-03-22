# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert fct_recibos lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `fct_recibos`
def upsert_fct_recibos(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO fct_recibos (
            id_recibo, id_origen_SIS, cod_recibo, id_dim_concepto_cobro, fecha_emision, fecha_vencimiento,
            fecha_pago, estado, importe_recibo, tiene_factura, forma_pago, id_dim_estudiante, id_dim_producto,
            id_fct_matricula, id_dim_programa, id_dim_modalidad, id_dim_institucion, id_dim_sede,
            id_dim_tipo_formacion, id_dim_tipo_negocio, fec_inicio_reconocimiento, fec_fin_reconocimiento,
            meses_reconocimiento, importe_Mensual_Reconocimiento, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_recibo) DO UPDATE SET
            cod_recibo = EXCLUDED.cod_recibo,
            id_origen_SIS = EXCLUDED.id_origen_SIS,
            id_dim_concepto_cobro = EXCLUDED.id_dim_concepto_cobro,
            fecha_emision = EXCLUDED.fecha_emision,
            fecha_vencimiento = EXCLUDED.fecha_vencimiento,
            fecha_pago = EXCLUDED.fecha_pago,
            estado = EXCLUDED.estado,
            importe_recibo = EXCLUDED.importe_recibo,
            tiene_factura = EXCLUDED.tiene_factura,
            forma_pago = EXCLUDED.forma_pago,
            id_dim_estudiante = EXCLUDED.id_dim_estudiante,
            id_dim_producto = EXCLUDED.id_dim_producto,
            id_fct_matricula = EXCLUDED.id_fct_matricula,
            id_dim_programa = EXCLUDED.id_dim_programa,
            id_dim_modalidad = EXCLUDED.id_dim_modalidad,
            id_dim_institucion = EXCLUDED.id_dim_institucion,
            id_dim_sede = EXCLUDED.id_dim_sede,
            id_dim_tipo_formacion = EXCLUDED.id_dim_tipo_formacion,
            id_dim_tipo_negocio = EXCLUDED.id_dim_tipo_negocio,
            fec_inicio_reconocimiento = EXCLUDED.fec_inicio_reconocimiento,
            fec_fin_reconocimiento = EXCLUDED.fec_fin_reconocimiento,
            meses_reconocimiento = EXCLUDED.meses_reconocimiento,
            importe_Mensual_Reconocimiento = EXCLUDED.importe_Mensual_Reconocimiento,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_recibo"], row["id_origen_SIS"], row["cod_recibo"], row["id_dim_concepto_cobro"],
            row["fecha_emision"], row["fecha_vencimiento"], row["fecha_pago"], row["estado"],
            row["importe_recibo"], row["tiene_factura"], row["forma_pago"], row["id_dim_estudiante"],
            row["id_dim_producto"], row["id_fct_matricula"], row["id_dim_programa"], row["id_dim_modalidad"],
            row["id_dim_institucion"], row["id_dim_sede"], row["id_dim_tipo_formacion"], row["id_dim_tipo_negocio"],
            row["fec_inicio_reconocimiento"], row["fec_fin_reconocimiento"], row["meses_reconocimiento"],
            row["importe_Mensual_Reconocimiento"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.fct_recibos` en Databricks
source_table = (spark.table("gold_lakehouse.fct_recibos")
                .select("id_recibo", "id_origen_SIS", "cod_recibo", "id_dim_concepto_cobro", "fecha_emision",
                        "fecha_vencimiento", "fecha_pago", "estado", "importe_recibo", "tiene_factura", "forma_pago",
                        "id_dim_estudiante", "id_dim_producto", "id_fct_matricula", "id_dim_programa", "id_dim_modalidad",
                        "id_dim_institucion", "id_dim_sede", "id_dim_tipo_formacion", "id_dim_tipo_negocio",
                        "fec_inicio_reconocimiento", "fec_fin_reconocimiento", "meses_reconocimiento",
                        "importe_Mensual_Reconocimiento", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_fct_recibos)
    logger.info("Proceso completado con éxito (Upsert en fct_recibos de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
