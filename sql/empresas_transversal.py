'''
PROYECTO:		    ANALÍTICA DE EMPRESAS Y NEGOCIOS
FRENTE DE TRABAJO:	EMPRESAS TRANSVERSAL
AUTOR:			    HITSS - FERNANDA ZAMBRANO
OPERACIÓN: 		    REPORTE QUE PERMITE IDENTIFICAR EL SEGMENTO DEL CLIENTE
VERSIÓN:	        V_1.0
FECHA:              23/07/2025
DESCRIPCIÓN:	    SCRIPT QUE PERMITE HACER CRUCE ENTRE LAS TABLAS DE ONIX Y ASIGNACIÓN PARA IDENTIFICAR EL SEGMENTO ACTUALIZADO DEL CLLIENTE.
'''

#Importación de librerias y módulos requeridos
import pandas as pd
import sys
import os

# Se inicia asignando a project_root la ruta actual
# agregando al sys.path para que puedan ser importados módulos desde diferentes ubicaciones
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from config.config import conIntelienciaComercial
from utils.file_exporter import FileExporter

# Clase para manejar la extracción de los datos
class EmpresasData:
    def __init__(self):
        self.db_connector = conIntelienciaComercial()   # Inicializa el conector de la base de datos

    def get_empresas_dataframe(self):                   # Ejecuta la consulta SQL
        query = """SELECT
                a."NIT",
                a."NIT_DV",
                c."iCompanyId" 		AS "ID_ONIX",
                a."RAZON_SOCIAL",  
                c."GRUPO_ECONOMICO" AS "GRUPO_OBJETIVO_ONIX",
                a."SEGMENTO_ACTUAL" AS "SEGMENTO_ASIGNACION",
                c."SEGMENTO" 		AS "SEGMENTO_ONIX",
                a."ESTADO_CLIENTE_FYM",
                a."CIUDAD",
                a."DEPARTAMENTO",
                c."AddressType" 	AS "SEDE",
                a."DIRECCION",
                c."Telefono" 		AS "TELEFONO",
                c."Correo" 			AS "CORREO_ELECTRONICO",
                LOWER (a."WEB") 	AS "WEB",
                UPPER (c."SECTOR") 	AS "SIC",
                c."ACTIVIDAD_ECO" 	AS "ACTIVIDAD_COMERCIAL",
                a."GO_TO_MARKET"
            FROM
                bd_production.tb_asignacion a
            LEFT JOIN
                public."tbl_Company" c ON a."NIT_DV" = c."NIT"
            WHERE
                a."SEGMENTO_CLIENTE" = 'EMPRESAS' """
        conn = self.db_connector.pg_ic_connect()        # Obtiene la conexión a la base de datos
        try:
            print("Ejecutando consulta en la base de datos...")
            df = pd.read_sql_query(query, conn)         # Almacena el resultado de la consulta en un DataFrame
            print("Consulta ejecutada con éxito.")
            return df
        except Exception as e:
            print(f"Ocurrió un error al ejecutar la consulta: {e}")
            return None

# Funcion que orquesta el flujo de trabajo completo
def run():
    print("Iniciando proceso de extracción de datos de empresas...")
    
    # Se crea una instancia de EmpresasData para obtener los datos
    empresas_data_source = EmpresasData()
    df_empresas = empresas_data_source.get_empresas_dataframe()
    
    # Se verifica si el DataFrame no es None y no está vacío
    if df_empresas is not None and not df_empresas.empty:
        print("Datos de empresas obtenidos con éxito.")
        print(df_empresas.head())
        
        # Se exporta el DataFrame a un archivo Excel
        exporter = FileExporter()
        exporter.export_to_excel(
            df=df_empresas,
            filename_prefix="reporte_empresas",
            path="C:/Users/46120442/OneDrive - GLOBAL HITSS/Documentos/REPORTES_PRUEBA/"
        )
    else:
        print("No se obtuvieron datos o el DataFrame está vacío. Proceso finalizado.")

if __name__ == "__main__":
    run()