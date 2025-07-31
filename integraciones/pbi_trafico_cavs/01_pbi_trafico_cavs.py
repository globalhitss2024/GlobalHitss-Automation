"""
PROYECTO:             EMPRESAS Y NEGOCIOS
AUTOR:                HITSS BI - JORGE MOSQUERA
OPERACION:            SCRIPT EN PYTHON CON LA ETL PARA EL PBI TRAICO CAVS
VERSION:              V_1.0
FECHA:                31/07/2025
DESCRIPCION:          SE CREA SCRIPT CON LAS EXTRACCIONES, TRANSFORMACIONES Y CARGUES DE DATA PARA EL PROCESO PBI TRAFICO CAVS
"""
# Librerías estándar
import Parametros as prm
import sys
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import text
# Importación de módulos personalizados y configuración de rutas
ruta_modulos = prm.ruta_modulos["ruta"]
ruta_modulos_1 = prm.ruta_modulos["rut_cl"]
if ruta_modulos not in sys.path:
    sys.path.append(ruta_modulos)
if ruta_modulos_1 not in sys.path:
    sys.path.append(ruta_modulos_1)    
# Importación de clases y funciones personalizadas
from config import conIntelienciaComercial
from Class_read_trsf_excel import ProcesadorBase

# Permite ver el todos los campos de un df
pd.set_option('display.max_columns', None)

# Configuración del logger para registrar mensajes informativos y errores
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


######################### Función principal MAIN orquesta la ejecución del proceso ETL#######################

def main():
    try:
        # Conexión a la base de datos
        engine = conIntelienciaComercial().pg_ic_connect()
        # Obtiene los datos auxiliares y nombre de la tabla auxiliar
        df_aux, nombre_tabla_aux = obtener_datos_aux(engine)
        # Crea instancia clase procesador
        procesador = ProcesadorBase(engine, df_aux, nombre_tabla_aux)
        # proceso pestanna RED MAESTRA
        #procesador.procesar_pestana("base_planta_comercial", transformar_Campos_redmaestra)
        # proceso pestanna RETAIL
        #procesador.procesar_pestana("base_pc_retail", transformar_campos_retail)


    except Exception as e:
        logger.error("Error general del proceso", exc_info=True)


if __name__ == '__main__':
    main()
