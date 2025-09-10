import logging
import pandas as pd

from integraciones.proceso_hfc.procesar_Ingresos_hfc import ProcesadorIngresosHFC
from integraciones.proceso_hfc.procesar_v360 import Procesar360
from integraciones.proceso_hfc.parametros import ruta_hfc, ruta_salida
from integraciones.proceso_hfc.activo_pym import ProcesarPYM
from integraciones.Proceso_Ingresos_Fo.homologacion_reader import HomologacionReader
from integraciones.proceso_hfc.parametros import (
    ruta_homologacion,
    ruta_pym,
    ruta_ingresos,
    ruta_salida,
    ruta_360,
    ruta_hfc,
    dic_columnas_ingresos_hfc,
    dic_360,
)

logger = logging.getLogger(__name__)


class ReglasRecurrenteHFC:
    def __init__(self, df_bd_asignacion: pd.DataFrame):
        self.df_hfc = ProcesadorIngresosHFC(ruta_ingresos, ruta_salida).procesar()
        self.df_pym = ProcesarPYM(ruta_pym, ruta_salida).procesar()
        self.df_360 = Procesar360(ruta_360, ruta_salida).procesar()
        self.df_bd_asignacion = df_bd_asignacion
        self.df_homologacion = HomologacionReader(ruta_homologacion).leer_homologacion()

    def ejecutar_reglas(self, df_bd_planta: pd.DataFrame) -> pd.DataFrame:
        """
        Por ahora solo devuelve df_bd_planta (placeholder).
        """
        logger.info("Ejecutar_reglas de HFC invocado (sin lógica todavía).")
        return df_bd_planta
