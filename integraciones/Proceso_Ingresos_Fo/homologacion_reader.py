import pandas as pd
import logging

logger = logging.getLogger(__name__)


class HomologacionReader:
    def __init__(self, ruta_archivo):
        self.ruta_archivo = ruta_archivo

    def leer_homologacion(self):
        """
        Lee el archivo de homologación y retorna un DataFrame
        solo con las columnas requeridas.

        Returns:
            DataFrame: Datos filtrados de homologación (vacío si no hay columnas).
        """
        # Filtrar solo las columnas necesarias en mayúsculas
        columnas_deseadas = [
            "Servicio",
            "Flia Final Col (Rent)V2",
            "Flia AMX_V2",
        ]

        try:
            logger.info(
                f"Iniciando lectura de archivo de homologación: {self.ruta_archivo}"
            )
            df_homologacion = pd.read_excel(self.ruta_archivo, header=1)

            # Eliminar duplicados ignorando la columna 'Cod'
            #df_homologacion = df_homologacion.drop_duplicates
            #(subset=df_homologacion.columns.difference(["Cod"]))

            #logger.info(
              #  f"Archivo homologación cargado con {len(df_homologacion)} "
              #  "registros y columnas filtradas."
            #)
            return df_homologacion

        except Exception as e:
            logger.error(f"Error al leer archivo de homologación: {e}")
            return pd.DataFrame(columns=columnas_deseadas)

