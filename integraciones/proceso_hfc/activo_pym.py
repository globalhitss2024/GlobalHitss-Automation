"""
===============================================================================
PROYECTO:      EMPRESAS Y NEGOCIOS
MÓDULO:        Procesamiento de archivo PYM
AUTOR:         Johana Pérez Montoya - HITSS BI
VERSIÓN:       1.0
FECHA:         06/08/2025
-------------------------------------------------------------------------------
DESCRIPCIÓN:
    Este módulo permite la extracción, validación y procesamiento del archivo
    PYM, garantizando la estandarización de columnas, eliminación de duplicados
    y generación de un archivo de salida listo para integraciones posteriores.
    
    Adicionalmente, implementa mecanismos de logging para trazabilidad y 
    asegura la creación de estructuras de salida en caso de que no existan.
-------------------------------------------------------------------------------
DEPENDENCIAS:
    - pandas
    - logging
    - os
    - sys
-------------------------------------------------------------------------------
MANTENIMIENTO:
    Equipo BI - HITSS
===============================================================================
"""


import pandas as pd
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from integraciones.proceso_hfc.parametros import dic_columnas_pym

logger = logging.getLogger(__name__)


class ProcesarPYM:
    def __init__(self, ruta_pym, ruta_salida):
        self.ruta_pym = ruta_pym
        self.ruta_salida = ruta_salida

    def procesar(self):
        try:
            # =======================
            # Crear carpeta de salida si no existe
            # =======================
            os.makedirs(self.ruta_salida, exist_ok=True)

            # =======================
            # Cargar archivo PYM
            # =======================
            #df = pd.read_csv(self.ruta_pym, sep=";", encoding="latin1")
            df = pd.read_excel(self.ruta_pym, engine="pyxlsb")
            df.columns = df.columns.str.strip().str.upper()

            logger.info(f"Archivo pym cargado con {len(df)} registros.")
            logger.info(f"Columnas en archivo pym: {list(df.columns)}")

            # =======================
            # Definir columnas deseadas
            # =======================
            columnas_deseadas = [col.upper() for col in dic_columnas_pym["columnas_deseadas"]]

            # =======================
            # Validar columnas faltantes y crearlas
            # =======================
            columnas_faltantes = [col for col in columnas_deseadas if col not in df.columns]
            if columnas_faltantes:
                logger.warning(f"Faltan columnas en el archivo PYM: {columnas_faltantes}")
                for col in columnas_faltantes:
                    df[col] = ""

            # Reordenar columnas
            df_pym = df[columnas_deseadas]

            # =======================
            # Capturar duplicados antes de eliminarlos
            # =======================
            duplicados = df_pym[df_pym.duplicated(keep=False)]
            if not duplicados.empty:
                ruta_duplicados = os.path.join(self.ruta_salida, "duplicados_eliminados.csv")
                duplicados.to_csv(ruta_duplicados, index=False, sep=";", encoding="utf-8-sig")
                logger.info(f"Duplicados guardados en: {ruta_duplicados}")
            else:
                logger.info("No se encontraron duplicados.")

            # =======================
            # Eliminar duplicados exactos
            # =======================
            df_pym = df_pym.drop_duplicates()
            logger.info(f"Registros después de eliminar duplicados: {len(df_pym)}")

            # =======================
            # Guardar archivo procesado
            # =======================
            archivo_salida = os.path.join(self.ruta_salida, "pym_procesado.csv")
            df_pym.to_csv(archivo_salida, index=False, sep=";", encoding="utf-8-sig")
            logger.info(f"Archivo procesado guardado en: {archivo_salida}")

            return df_pym

        except Exception as e:
            logger.error(f"Error al procesar archivo PYM: {e}", exc_info=True)
            raise
