"""
PROYECTO:        EMPRESAS Y NEGOCIOS
AUTOR:           HITSS BI - Johana Perez Montoya
VERSIÓN:         V_1.2
FECHA:           06/08/2025
DESCRIPCIÓN:     Orquesta el procesamiento de archivos CICLOS,
                 RI y datos desde BD planta comercial.
"""

import sys
import os
import logging
import pandas as pd
from datetime import datetime, timedelta

sys.path.append(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
)

from config.config import conDbInteligenciaComercial as conndbi
from config.config import conIntelienciaComercial
from utils.file_exporter import FileExporter
from integraciones.Proceso_Ingresos_Fo.Parametros import (
    ruta_ciclos,
    ruta_ri,
    ruta_salida,
    ruta_homologacion,
)
from integraciones.Proceso_Ingresos_Fo.reglas_ri_detalle import RiDetalle
from sql.ingresos_fo_hfc.bd_asignacion import query_asignacion
from sql.ingresos_fo_hfc.bd_planta import consulta_planta


# ==============================
# Configuración de logging
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ==============================
# Fecha de referencia
# ==============================
current_datetime = datetime.now()
time_delta = current_datetime - timedelta(days=30)
formatted_datetime = time_delta.strftime("%Y-%m-01")
logger.info(f"Fecha de referencia: {formatted_datetime}")


# ==============================
# Funciones de BD
# ==============================
def read_data_bd_planta():
    """Lee los datos de Planta Comercial desde la BD."""
    try:
        engine = conndbi().pg_ic_connect()
        query = consulta_planta()

        with engine.connect() as conn:
            df_bd_planta = pd.read_sql(query, conn)

        return df_bd_planta

    except Exception as e:
        logger.error(f"Error al leer datos de BD Planta: {e}")
        raise


def read_data_bd_asignacion():
    """Lee los datos de Asignación desde la BD (conexión IC)."""
    try:
        engine = conIntelienciaComercial().pg_ic_connect()
        query = query_asignacion()

        with engine.connect() as conn:
            df_bd_asignacion = pd.read_sql(query, conn)

        engine.dispose()

        df_bd_asignacion = (
            df_bd_asignacion.sort_values(by="NIT_DV")
            .drop_duplicates(subset=["NIT_DV"], keep="first")
        )

        return df_bd_asignacion

    except Exception as e:
        raise RuntimeError(f"Error al leer datos de asignación: {e}")


# ==============================
# Main Orquestador
# ==============================
def main():
    """Orquesta el flujo principal de procesamiento."""
    try:
        logger.info("==== INICIO PROCESO EMPRESAS Y NEGOCIOS ====")

        # 1. Consultar datos de BD Planta
        logger.info("Leyendo datos de BD Planta Comercial...")
        df_bd_planta = read_data_bd_planta()
        logger.info(
            f"Datos de Planta Comercial leídos: {len(df_bd_planta)} registros."
        )
        

        # 2. Consultar datos de BD Asignación
        df_bd_asignacion = read_data_bd_asignacion()
        logger.info(
            f"Datos de Asignación leídos: {len(df_bd_asignacion)} registros."
        )
        

        # 3. Ejecutar reglas con RiDetalle
        logger.info("Ejecutando reglas RI Detalle...")
        ri_detalle = RiDetalle(df_bd_asignacion)
        df_resultado = ri_detalle.ejecutar_reglas(df_bd_planta)

        # 4. Exportar resultado
        FileExporter().export_to_excel(df_resultado, "RI_DETALLE", ruta_salida)

        logger.info("==== FIN   PROCESO FO EXITOSO ====")

    except Exception as e:
        logger.error(f"Error en el proceso principal: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

