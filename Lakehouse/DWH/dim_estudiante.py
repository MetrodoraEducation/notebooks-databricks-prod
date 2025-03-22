# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_estudiante lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_estudiante`
def upsert_dim_estudiante(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión desde la función del Bloque 1
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_estudiante (
            id_dim_estudiante,
            id_origen_sis,
            cod_estudiante,
            nombre_estudiante,
            email,
            phone,
            fecha_creacion,
            estado,
            edad,
            id_zoho,
            pais,
            ciudad,
            codigo_postal,
            direccion_postal,
            ETLcreatedDate,
            ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_dim_estudiante) DO UPDATE SET
            id_origen_sis = EXCLUDED.id_origen_sis,
            cod_estudiante = EXCLUDED.cod_estudiante,
            nombre_estudiante = EXCLUDED.nombre_estudiante,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            fecha_creacion = EXCLUDED.fecha_creacion,
            estado = EXCLUDED.estado,
            edad = EXCLUDED.edad,
            id_zoho = EXCLUDED.id_zoho,
            pais = EXCLUDED.pais,
            ciudad = EXCLUDED.ciudad,
            codigo_postal = EXCLUDED.codigo_postal,
            direccion_postal = EXCLUDED.direccion_postal,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_dim_estudiante"],
            row["id_origen_sis"],
            row["cod_estudiante"],
            row["nombre_estudiante"],
            row["email"],
            row["phone"],
            row["fecha_creacion"],
            row["estado"],
            row["edad"],
            row["id_zoho"],
            row["pais"],
            row["ciudad"],
            row["codigo_postal"],
            row["direccion_postal"],
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

# Leer datos desde la tabla `gold_lakehouse.dim_estudiante` en Databricks
source_table = (spark.table("gold_lakehouse.dim_estudiante")
                .select(
                    "id_dim_estudiante",
                    "id_origen_sis",
                    "cod_estudiante",
                    "nombre_estudiante",
                    "email",
                    "phone",
                    "fecha_creacion",
                    "estado",
                    "edad",
                    "id_zoho",
                    "pais",
                    "ciudad",
                    "codigo_postal",
                    "direccion_postal",
                    "ETLcreatedDate",
                    "ETLupdatedDate"
                ))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_estudiante)
    logger.info("Proceso completado con éxito (Upsert en dim_estudiante de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
