"""
PROYECTO:           ANALÍTICA DE EMPRESAS Y NEGOCIOS
FRENTE DE TRABAJO:  EMPRESAS TRANSVERSAL
AUTOR:              HITSS - FERNANDA ZAMBRANO
OPERACIÓN:          REPORTE QUE PERMITE IDENTIFICAR EL SEGMENTO DEL CLIENTE
VERSIÓN:            V_1.0
FECHA:              24/07/2025
DESCRIPCIÓN:        SCRIPT QUE ORQUESTA EL FLUJO DE LA APLICACIÓN.
"""

# Importación de librerías y módulos requeridos
import pandas as pd
import sys
import os

# Se inicia asignando a project_root la ruta actual
# agregando al sys.path para que puedan ser importados módulos desde diferentes ubicaciones
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from config.config import conIntelienciaComercial
from utils.file_exporter import FileExporter
from sql.empresas_transversal import get_query

# --- Clase para la Extracción de Datos ---
class EmpresasData:
    def __init__(self):
        self.db_connector = conIntelienciaComercial()

    def get_empresas_dataframe(self):
        # Obtiene la consulta SQL del módulo importado.
        query = get_query()
        # Establece la conexión a la base de datos.
        conn = self.db_connector.pg_ic_connect()
        try:
            print("Ejecutando consulta en la base de datos...")
            # Ejecuta la consulta y carga los resultados en un DataFrame.
            df = pd.read_sql_query(query, conn)
            print("Consulta ejecutada con éxito.")
            return df
        except Exception as e:
            print(f"Ocurrió un error al ejecutar la consulta: {e}")
            return None

# Funcion que orquesta el flujo de trabajo completo
def run():
    print("Iniciando proceso de extracción de datos de empresas...")
    
    # Se crea una instancia de EmpresasData para obtener los datos.
    empresas_data_source = EmpresasData()
    df_empresas = empresas_data_source.get_empresas_dataframe()
    
    # Se verifica si el DataFrame no es None y no está vacío.
    if df_empresas is not None and not df_empresas.empty:
        print("Datos de empresas obtenidos con éxito.")
        print(df_empresas.head())
        
        # Se exporta el DataFrame a un archivo Excel.
        exporter = FileExporter()
        exporter.export_to_excel(
            df=df_empresas,
            filename_prefix="REPORTE_EMPRESAS",
            path="C:/Users/46120442/OneDrive - GLOBAL HITSS/Documentos/REPORTES_PRUEBA/"
        )
    else:
        print("No se obtuvieron datos o el DataFrame está vacío. Proceso finalizado.")

if __name__ == "__main__":
    run()