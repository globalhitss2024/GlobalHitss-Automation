import logging

import numpy as np
import pandas as pd

from utils.file_exporter import FileExporter
from integraciones.Proceso_Ingresos_Fo.Parametros import dic_columnas_ciclos

logger = logging.getLogger(__name__)


def _plaintext_series(s: pd.Series) -> pd.Series:
    """Limpia espacios y convierte valores a texto plano."""
    return (
        s.astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .replace({"nan": ""})
    )


class ProcesadorCiclos:
    def __init__(self, ruta_ciclos: str, ruta_salida: str):
        self.ruta_ciclos = ruta_ciclos
        self.ruta_salida = ruta_salida
        self.df_ciclos = None
        self.exportador = FileExporter()

    def procesar(self) -> pd.DataFrame:
        """Lee el archivo de CICLOS, limpia columnas y exporta."""
        try:
            logger.info(f"Leyendo archivo CICLOS desde: {self.ruta_ciclos}")
            self.df_ciclos = pd.read_excel(self.ruta_ciclos, sheet_name="Hoja1")

            logger.info(
                f"Columnas originales en CICLOS: {list(self.df_ciclos.columns)}"
            )

            # Normalizar nombres de columnas
            self.df_ciclos.columns = self.df_ciclos.columns.str.strip().str.lower()

            # Renombrar columnas según diccionario
            self.df_ciclos = self.df_ciclos.rename(
                columns=dic_columnas_ciclos["rename"]
            )

            # Filtrar columnas deseadas
            self.df_ciclos = self.df_ciclos[
                dic_columnas_ciclos["columnas_deseadas"]
            ]

            # Limpiar columnas
            for col in self.df_ciclos.columns:
                self.df_ciclos[col] = _plaintext_series(self.df_ciclos[col])

            # Eliminar duplicados
            self.df_ciclos = self.df_ciclos.drop_duplicates()
            logger.info(
                f"Registros después de eliminar duplicados: {len(self.df_ciclos)}"
            )

            logger.info("Exportando archivo CICLOS filtrado...")
            self.exportador.export_to_excel(
                self.df_ciclos, "CICLOS_EXPORT", self.ruta_salida
            )

            return self.df_ciclos

        except Exception as e:
            logger.error(f"Error procesando CICLOS: {str(e)}", exc_info=True)
            raise
