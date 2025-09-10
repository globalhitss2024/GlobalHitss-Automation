import pandas as pd
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from integraciones.proceso_hfc.parametros import dic_360

logger = logging.getLogger(__name__)


class Procesar360:
    def __init__(self, ruta_ingresos, ruta_salida):
        self.ruta_ingresos = ruta_ingresos
        self.ruta_salida = ruta_salida

    def procesar(self):
        try:
            # =======================
            # Crear carpeta de salida si no existe
            # =======================
            os.makedirs(self.ruta_salida, exist_ok=True)

            # =======================
            # Cargar archivo 360
            # =======================
            df = pd.read_csv(self.ruta_ingresos, sep=";", encoding="latin1")
            df.columns = df.columns.str.strip().str.upper()

            logger.info(f"Archivo 360 cargado con {len(df)} registros.")
            logger.info(f"Columnas en archivo 360: {list(df.columns)}")

            # =======================
            # Definir columnas deseadas
            # =======================
            columnas_deseadas = [col.upper() for col in dic_360["columnas_deseadas"]]

            # =======================
            # Validar columnas faltantes y crearlas
            # =======================
            columnas_faltantes = [col for col in columnas_deseadas if col not in df.columns]
            if columnas_faltantes:
                logger.warning(f"Faltan columnas en el archivo 360: {columnas_faltantes}")
                for col in columnas_faltantes:
                    df[col] = ""

            # Reordenar columnas
            df_360 = df[columnas_deseadas]

            # =======================
            # Capturar duplicados antes de eliminarlos
            # =======================
            duplicados = df_360[df_360.duplicated(keep=False)]
            if not duplicados.empty:
                ruta_duplicados = os.path.join(self.ruta_salida, "duplicados_eliminados.csv")
                duplicados.to_csv(ruta_duplicados, index=False, sep=";", encoding="utf-8-sig")
                logger.info(f"Duplicados guardados en: {ruta_duplicados}")
            else:
                logger.info("No se encontraron duplicados.")

            # =======================
            # Eliminar duplicados exactos
            # =======================
            df_360 = df_360.drop_duplicates()
            logger.info(f"Registros después de eliminar duplicados: {len(df_360)}")

            # =======================
            # Guardar archivo procesado
            # =======================
            archivo_salida = os.path.join(self.ruta_salida, "360_procesado.csv")
            df_360.to_csv(archivo_salida, index=False, sep=";", encoding="utf-8-sig")
            logger.info(f"Archivo procesado guardado en: {archivo_salida}")

            return df_360

        except Exception as e:
            logger.error(f"Error al procesar archivo 360: {e}", exc_info=True)
            raise
