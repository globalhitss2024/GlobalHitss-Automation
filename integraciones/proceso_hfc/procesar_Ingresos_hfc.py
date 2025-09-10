import pandas as pd
import logging
import os
import sys

# Agregar la raíz del proyecto al sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importa desde la ruta correcta
from integraciones.proceso_hfc.parametros import dic_columnas_ingresos_hfc

logger = logging.getLogger(__name__)


class ProcesadorIngresosHFC:
    def __init__(self, ruta_ingresos, ruta_salida):
        self.ruta_ingresos = ruta_ingresos
        self.ruta_salida = ruta_salida

    def procesar(self):
        try:
            # =======================
            # Cargar archivo ingresos hfc
            # =======================
            df = pd.read_csv(self.ruta_ingresos, encoding="latin1", sep=";")
            df = df.applymap(lambda x: x.encode("latin1").decode("utf-8") if isinstance(x, str) else x)


            #df = pd.read_csv(self.ruta_ingresos, encoding='latin1', sep=';')
            #df.columns = df.columns.str.strip().str.upper()

            logger.info(f"Archivo ingresos cargado con {len(df)} registros.")
            logger.info(f"Columnas reales en archivo ingresos: {list(df.columns)}")

            # =======================
            # Definir columnas esperadas
            # =======================
            df.columns = df.columns.str.strip().str.upper()
            columnas_deseadas = [col.upper() for col in dic_columnas_ingresos_hfc["columnas_deseadas"]]


            # =======================
            # Validar columnas faltantes y crearlas
            # =======================
            columnas_faltantes = [col for col in columnas_deseadas if col not in df.columns]
            if columnas_faltantes:
                logger.warning(f"Faltan columnas en el archivo ingresos hfc: {columnas_faltantes}")
                for col in columnas_faltantes:
                    df[col] = ""

            # Reordenar el DataFrame para que quede con todas las columnas en el orden correcto
            df_ingresos = df[columnas_deseadas]

            # =======================
            # Eliminar duplicados
            # =======================
            df_ingresos = df_ingresos.drop_duplicates()
            logger.info(f"Registros después de eliminar duplicados: {len(df_ingresos)}")

            duplicados = df[df.duplicated(keep=False)]
            duplicados.to_csv(os.path.join(self.ruta_salida, "duplicados_eliminados.csv"), index=False, sep=";", encoding="utf-8-sig")

            # =======================
            # Guardar salida en CSV
            # =======================
            os.makedirs(self.ruta_salida, exist_ok=True)  # crea carpeta si no existe
            archivo_salida = os.path.join(self.ruta_salida, "ingresos_K-H.csv")

            df_ingresos.to_csv(archivo_salida, index=False, sep=";", encoding="utf-8-sig")

            logger.info(f"Archivo procesado guardado en: {archivo_salida}")

            return df_ingresos

        except Exception as e:
            logger.error(f"Error al procesar archivo HFC: {e}", exc_info=True)
            raise
