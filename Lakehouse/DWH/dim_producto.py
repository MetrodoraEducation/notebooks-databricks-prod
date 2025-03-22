# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_escenario_budget lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_producto`
def upsert_dim_producto(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_producto (
            id_Dim_Producto, cod_Producto_Origen, cod_Producto_Corto, cod_Producto, origen_Producto,
            tipo_Producto, area, nombre_Oficial, curso, numero_Curso, codigo_sede, sede, fecha_Inicio_Curso, fecha_Fin_Curso, 
            fecha_inicio_reconocimiento, fecha_fin_reconocimiento,
            ciclo_id, num_Plazas, num_Grupo, vertical, cod_Vertical, especialidad, cod_Especialidad,
            num_Creditos, cod_Programa, admite_Admision, tipo_Negocio, acreditado, nombre_Web, entidad_Legal,
            cod_entidad_Legal, modalidad, cod_Modalidad, fecha_Inicio, fecha_Fin, meses_Duracion,
            horas_Acreditadas, horas_Presenciales, fecha_Inicio_Pago, fecha_Fin_Pago, num_Cuotas,
            importe_Certificado, importe_Ampliacion, importe_Docencia, importe_Matricula, importe_Total,
            ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_Dim_Producto) DO UPDATE SET
            cod_Producto_Origen = EXCLUDED.cod_Producto_Origen,
            cod_Producto_Corto = EXCLUDED.cod_Producto_Corto,
            cod_Producto = EXCLUDED.cod_Producto,
            origen_Producto = EXCLUDED.origen_Producto,
            tipo_Producto = EXCLUDED.tipo_Producto,
            area = EXCLUDED.area,
            nombre_Oficial = EXCLUDED.nombre_Oficial,
            curso = EXCLUDED.curso,
            numero_Curso = EXCLUDED.numero_Curso,
            codigo_sede = EXCLUDED.codigo_sede,
            sede = EXCLUDED.sede,
            fecha_Inicio_Curso = EXCLUDED.fecha_Inicio_Curso,
            fecha_Fin_Curso = EXCLUDED.fecha_Fin_Curso,
            fecha_inicio_reconocimiento = EXCLUDED.fecha_inicio_reconocimiento,
            fecha_fin_reconocimiento = EXCLUDED.fecha_fin_reconocimiento,
            ciclo_id = EXCLUDED.ciclo_id,
            num_Plazas = EXCLUDED.num_Plazas,
            num_Grupo = EXCLUDED.num_Grupo,
            vertical = EXCLUDED.vertical,
            cod_Vertical = EXCLUDED.cod_Vertical,
            especialidad = EXCLUDED.especialidad,
            cod_Especialidad = EXCLUDED.cod_Especialidad,
            num_Creditos = EXCLUDED.num_Creditos,
            cod_Programa = EXCLUDED.cod_Programa,
            admite_Admision = EXCLUDED.admite_Admision,
            tipo_Negocio = EXCLUDED.tipo_Negocio,
            acreditado = EXCLUDED.acreditado,
            nombre_Web = EXCLUDED.nombre_Web,
            entidad_Legal = EXCLUDED.entidad_Legal,
            cod_entidad_Legal = EXCLUDED.cod_entidad_Legal,
            modalidad = EXCLUDED.modalidad,
            cod_Modalidad = EXCLUDED.cod_Modalidad,
            fecha_Inicio = EXCLUDED.fecha_Inicio,
            fecha_Fin = EXCLUDED.fecha_Fin,
            meses_Duracion = EXCLUDED.meses_Duracion,
            horas_Acreditadas = EXCLUDED.horas_Acreditadas,
            horas_Presenciales = EXCLUDED.horas_Presenciales,
            fecha_Inicio_Pago = EXCLUDED.fecha_Inicio_Pago,
            fecha_Fin_Pago = EXCLUDED.fecha_Fin_Pago,
            num_Cuotas = EXCLUDED.num_Cuotas,
            importe_Certificado = EXCLUDED.importe_Certificado,
            importe_Ampliacion = EXCLUDED.importe_Ampliacion,
            importe_Docencia = EXCLUDED.importe_Docencia,
            importe_Matricula = EXCLUDED.importe_Matricula,
            importe_Total = EXCLUDED.importe_Total,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_Dim_Producto"], row["cod_Producto_Origen"], row["cod_Producto_Corto"], row["cod_Producto"],
            row["origen_Producto"], row["tipo_Producto"], row["area"], row["nombre_Oficial"],
            row["curso"], row["numero_Curso"], row["codigo_sede"], row["sede"], row["fecha_Inicio_Curso"], row["fecha_Fin_Curso"],
            row["fecha_inicio_reconocimiento"], row["fecha_fin_reconocimiento"],
            row["ciclo_id"], row["num_Plazas"], row["num_Grupo"], row["vertical"],
            row["cod_Vertical"], row["especialidad"], row["cod_Especialidad"], row["num_Creditos"],
            row["cod_Programa"], row["admite_Admision"], row["tipo_Negocio"], row["acreditado"],
            row["nombre_Web"], row["entidad_Legal"], row["cod_entidad_Legal"], row["modalidad"],
            row["cod_Modalidad"], row["fecha_Inicio"], row["fecha_Fin"], row["meses_Duracion"],
            row["horas_Acreditadas"], row["horas_Presenciales"], row["fecha_Inicio_Pago"], row["fecha_Fin_Pago"],
            row["num_Cuotas"], row["importe_Certificado"], row["importe_Ampliacion"], row["importe_Docencia"],
            row["importe_Matricula"], row["importe_Total"], row["ETLcreatedDate"], row["ETLupdatedDate"]
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

# Leer datos desde la tabla `gold_lakehouse.dim_producto` en Databricks
source_table = (spark.table("gold_lakehouse.dim_producto")
                .select("id_Dim_Producto", "cod_Producto_Origen", "cod_Producto_Corto", "cod_Producto",
                        "origen_Producto", "tipo_Producto", "area", "nombre_Oficial", "curso", "numero_Curso",
                        "codigo_sede", "sede",
                        "fecha_Inicio_Curso", "fecha_Fin_Curso", "fecha_inicio_reconocimiento", "fecha_fin_reconocimiento", "ciclo_id", "num_Plazas", "num_Grupo",
                        "vertical", "cod_Vertical", "especialidad", "cod_Especialidad", "num_Creditos",
                        "cod_Programa", "admite_Admision", "tipo_Negocio", "acreditado", "nombre_Web",
                        "entidad_Legal", "cod_entidad_Legal", "modalidad", "cod_Modalidad", "fecha_Inicio",
                        "fecha_Fin", "meses_Duracion", "horas_Acreditadas", "horas_Presenciales",
                        "fecha_Inicio_Pago", "fecha_Fin_Pago", "num_Cuotas", "importe_Certificado",
                        "importe_Ampliacion", "importe_Docencia", "importe_Matricula", "importe_Total",
                        "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_producto)
    logger.info("Proceso completado con éxito (Upsert en dim_producto de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
