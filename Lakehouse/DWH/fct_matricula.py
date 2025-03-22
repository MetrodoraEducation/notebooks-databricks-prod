# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert fct_matricula lakehouse postgresql
from pyspark.sql.functions import col
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `fct_matricula`
def upsert_fct_matricula(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión desde la función del Bloque 1
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO fct_matricula (
            id_matricula,
            id_origen_SIS,
            cod_matricula,
            id_dim_estudiante,
            id_dim_programa,
            id_dim_modalidad,
            id_dim_institucion,
            id_dim_sede,
            id_dim_producto,
            id_dim_tipo_formacion,
            id_dim_tipo_negocio,
            id_dim_pais,
            ano_curso,
            fec_matricula,
            id_dim_estado_matricula,
            fec_anulacion,
            fec_finalizacion,
            nota_media,
            cod_descuento,
            importe_matricula,
            importe_descuento,
            importe_cobros,
            tipo_pago,
            edad_acceso,
            fec_ultimo_login_LMS,
            zoho_deal_id,
            ETLcreatedDate,
            ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_matricula) DO UPDATE SET
            id_origen_SIS = EXCLUDED.id_origen_SIS,
            cod_matricula = EXCLUDED.cod_matricula,
            id_dim_estudiante = EXCLUDED.id_dim_estudiante,
            id_dim_programa = EXCLUDED.id_dim_programa,
            id_dim_modalidad = EXCLUDED.id_dim_modalidad,
            id_dim_institucion = EXCLUDED.id_dim_institucion,
            id_dim_sede = EXCLUDED.id_dim_sede,
            id_dim_producto = EXCLUDED.id_dim_producto,
            id_dim_tipo_formacion = EXCLUDED.id_dim_tipo_formacion,
            id_dim_tipo_negocio = EXCLUDED.id_dim_tipo_negocio,
            id_dim_pais = EXCLUDED.id_dim_pais,
            ano_curso = EXCLUDED.ano_curso,
            fec_matricula = EXCLUDED.fec_matricula,
            id_dim_estado_matricula = EXCLUDED.id_dim_estado_matricula,
            fec_anulacion = EXCLUDED.fec_anulacion,
            fec_finalizacion = EXCLUDED.fec_finalizacion,
            nota_media = EXCLUDED.nota_media,
            cod_descuento = EXCLUDED.cod_descuento,
            importe_matricula = EXCLUDED.importe_matricula,
            importe_descuento = EXCLUDED.importe_descuento,
            importe_cobros = EXCLUDED.importe_cobros,
            tipo_pago = EXCLUDED.tipo_pago,
            edad_acceso = EXCLUDED.edad_acceso,
            fec_ultimo_login_LMS = EXCLUDED.fec_ultimo_login_LMS,
            zoho_deal_id = EXCLUDED.zoho_deal_id,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_matricula"],
            row["id_origen_SIS"],
            row["cod_matricula"],
            row["id_dim_estudiante"],
            row["id_dim_programa"],
            row["id_dim_modalidad"],
            row["id_dim_institucion"],
            row["id_dim_sede"],
            row["id_dim_producto"],
            row["id_dim_tipo_formacion"],
            row["id_dim_tipo_negocio"],
            row["id_dim_pais"],
            row["ano_curso"],
            row["fec_matricula"],
            row["id_dim_estado_matricula"],
            row["fec_anulacion"],
            row["fec_finalizacion"],
            row["nota_media"],
            row["cod_descuento"],
            row["importe_matricula"],
            row["importe_descuento"],
            row["importe_cobros"],
            row["tipo_pago"],
            row["edad_acceso"],
            row["fec_ultimo_login_LMS"],
            row["zoho_deal_id"],
            row["ETLcreatedDate"],
            row["ETLupdatedDate"]
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

# Leer datos desde la tabla `gold_lakehouse.fct_matricula` en Databricks
source_table = (spark.table("gold_lakehouse.fct_matricula")
                .filter(col("id_matricula") != -1)
                .select(
                    "id_matricula",
                    "id_origen_SIS",
                    "cod_matricula",
                    "id_dim_estudiante",
                    "id_dim_programa",
                    "id_dim_modalidad",
                    "id_dim_institucion",
                    "id_dim_sede",
                    "id_dim_producto",
                    "id_dim_tipo_formacion",
                    "id_dim_tipo_negocio",
                    "id_dim_pais",
                    "ano_curso",
                    "fec_matricula",
                    "id_dim_estado_matricula",
                    "fec_anulacion",
                    "fec_finalizacion",
                    "nota_media",
                    "cod_descuento",
                    "importe_matricula",
                    "importe_descuento",
                    "importe_cobros",
                    "tipo_pago",
                    "edad_acceso",
                    "fec_ultimo_login_LMS",
                    "zoho_deal_id",
                    "ETLcreatedDate",
                    "ETLupdatedDate"
                ))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_fct_matricula)
    logger.info("Proceso completado con éxito (Upsert en fct_matricula de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

