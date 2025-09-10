import pandas as pd
import logging

from integraciones.Proceso_Ingresos_Fo.Parametros import dic_columnas_ri

logger = logging.getLogger(__name__)


class ProcesadorRI:
    def __init__(self, ruta_ri, ruta_salida):
        self.ruta_ri = ruta_ri
        self.ruta_salida = ruta_salida

    def procesar(self):
        try:
            # =======================
            # Cargar archivo RI
            # =======================
            df = pd.read_excel(self.ruta_ri)
            logger.info(f"Archivo RI cargado con {len(df)} registros.")
            logger.info(f"Columnas reales en archivo RI: {list(df.columns)}")

            # =======================
            # Definir columnas esperadas
            # =======================
            columnas_deseadas = dic_columnas_ri["columnas_deseadas"]

            # =======================
            # Validar columnas faltantes
            # =======================
            columnas_faltantes = [col for col in columnas_deseadas if col not in df.columns]
            if columnas_faltantes:
                logger.warning(f"Faltan columnas en el archivo RI: {columnas_faltantes}")
                for col in columnas_faltantes:
                    df[col] = ""

            df_ri = df[columnas_deseadas]

            # =======================
            # Crear columna concatenada
            # =======================
            df_ri["CONTENADO_ID"] = (
                df_ri["Enlace"].astype(str).str.strip() + "-" +
                df_ri["ID Onyx Propietario"].astype(str).str.strip()
            )

            # =======================
            # Eliminar duplicados (opcional)
            # =======================
            # df_ri = df_ri.drop_duplicates()
            # logger.info(f"Registros después de eliminar duplicados: {len(df_ri)}")

            return df_ri

        except Exception as e:
            logger.error(f"Error al procesar archivo RI: {e}", exc_info=True)
            raise