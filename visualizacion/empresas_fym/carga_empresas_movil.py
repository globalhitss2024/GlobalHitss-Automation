'''
PROYECTO:               ANALÍTICA DE EMPRESAS Y NEGOCIOS
FRENTE DE TRABAJO:      VISUALIZACIÓN
AUTOR:                  HITSS - FERNANDA ZAMBRANO
OPERACIÓN:              SCRIPT DE CARGA DE TABLA
VERSIÓN:                v_1.0
FECHA:                  25/08/2025
DESCRIPCIÓN:            SCRIPT QUE PERMITE LA CARGA DE LA TABLA tb_ejecucion_emp_movil
'''

import os
import sys
import pandas as pd
from sqlalchemy import text
import re

# Se define la ruta absoluta del proyecto y se agrega a sys.path para encontrar los módulos.
project_root = 'C:/Users/46120442/OneDrive - GLOBAL HITSS/Documentos/Proyectos Empresas y Negocios/Tableros/'
sys.path.insert(0, project_root)

from config.config import conDbInteligenciaComercial
from utils.Class_read_trsf_excel import excel_extractor, load_update_Datos

def main():
    """
    Proceso principal para cargar los datos desde Excel a la base de datos.
    """
    # Se construye la ruta relativa hacia el archivo Excel
    excel_file_path = os.path.join(project_root, 'visualizacion', 'Bases Empresas Fijo y Movil.xlsx')

    # Variables de configuración
    db_schema = 'sch_emp_movil'
    db_table = 'tb_ejecucion_emp_movil'
    excel_range = 'A:AD' # Columnas A hasta AD, todas las filas

    engine = None
    try:
        # Conexión a la Base de Datos
        db_connector = conDbInteligenciaComercial()
        engine = db_connector.pg_ic_connect()
        print("Conexión exitosa.")    

        # Limpieza de la Tabla antes de cargar nuevos datos.
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {db_schema}.{db_table} RESTART IDENTITY"))
            conn.commit()

        # Obtener todos los nombres de las hojas del archivo Excel
        xls = pd.ExcelFile(excel_file_path)
        all_sheet_names = xls.sheet_names
        xls.close()

        sheet_pattern = re.compile(r'^Empresas Movil \d{4}$') # Expresión regular para filtrar las hojas que coinciden con "Empresas Movil" seguido de 4 dígitos 
        sheets_to_process = [sheet for sheet in all_sheet_names if sheet_pattern.match(sheet)]

        if not sheets_to_process:
            print("No se encontraron hojas para Empresas Movil.")
            return
        
        # Iterar sobre las hojas filtradas para cargar datos
        for excel_sheet in sheets_to_process:
            print(f"Procesando hoja: {excel_sheet}")
            try:
                # Instanciar excel_extractor para la extracción de datos
                extractor = excel_extractor(
                    archivo=excel_file_path, 
                    hoja_rango=f'{excel_sheet}!{excel_range}'
                )
                df_excel = extractor.obtener_datos()  # Extraer datos del Excel
                df_excel = df_excel.dropna(how='all')  # Elimina filas donde todos los valores son nulos
                df_excel.columns = df_excel.columns.str.strip()  # Limpiar espacios en los nombres de columnas
                print(f"Se extrajeron {df_excel.shape[0]} filas del Excel.")
                # Mapeo de nombres de columnas de Excel a nombres de columnas de la base de datos
                
                column_mapping = {
                    'TIPO': 'TIPO',
                    'MES' : 'MES',
                    'AÑO' : 'ANO',
                    'ZONA' : 'ZONA',
                    'RAZON' : 'RAZON',
                    'DONANTE_RECEPTOR' : 'DONANTE_RECEPTOR',
                    'TRANSACCIONAL' : 'TRANSACCIONAL',
                    'RAZON SOCIAL' : 'RAZON_SOCIAL',
                    'NIT' : 'NIT',
                    'RANGO CFM' : 'RANGO_CFM',
                    'CODIGO' : 'CODIGO',
                    'NOMBRE CONSULTOR' : 'NOMBRE_CONSULTOR',
                    'PLAN' : 'PLAN',
                    'IDENTIF_MTR' : 'IDENTIF_MTR',
                    'GERENCIA QUE CUENTA' : 'CUENTA_GERENCIA',
                    'NOMBRE COORDINADOR' : 'COORDINADOR',
                    'TIPO/BASE' : 'TIPO_BASE',
                    'CFM$' : 'CFM',
                    'LINEAS' : 'LINEAS',
                    'GERENCIA BASE' : 'GERENCIA_BASE',
                    'DIRECCION  QUE CUENTA' : 'CUENTA_DIRECCION',
                    'DIRECCION BASE' : 'DIRECCION_BASE',
                    'CEDULA_CONS' : 'CEDULA_CONS',
                    'CEDULA COORD' : 'CEDULA_COORD',
                    'CEDULA GERENTE' : 'CEDULA_GERENTE',
                    'DETALLE' : 'DETALLE',
                    'SPLIT BILLING' : 'SPLIT_BILLING',
                    'DISTRIBUIDOR' : 'DISTRIBUIDOR',
                    'FAMILIA DE PRODUCTOS' : 'FAMILIA_PRODUCTOS',
                    '#  LINEAS' : 'NRO_LINEAS'
                }

                # Renombrar las columnas del DataFrame
                df_excel = df_excel.rename(columns=column_mapping)

                # Limpiar la columna 'CFM' para que solo tenga valores numéricos y no $
                if 'CFM' in df_excel.columns:
                    df_excel['CFM'] = (
                        df_excel['CFM']
                        .replace(r'[^\d\.\-]+', '', regex=True)  # Elimina caracteres no numéricos excepto punto y guion
                        .replace('-', None)                      # Convierte '-' en None
                        .replace('', None)                       # Convierte cadenas vacías en None
                        .astype(float)                           # Convierte a float
                    )

                # Carga de Datos en la Base de Datos
                loader = load_update_Datos(engine_conexion=engine)
                loader.cargar_df_a_tabla(df_tabla=df_excel, name=db_table, schema=db_schema)
                print(f"Carga Exitosa! Se insertaron {df_excel.shape[0]} registros de la hoja '{excel_sheet}'.")

            except Exception as e:
                print(f"ERROR: No se pudo procesar la hoja '{excel_sheet}': {e}")
                continue

    except FileNotFoundError as e:
        print(f"ERROR: No se pudo encontrar el archivo: {e}")
    except Exception as e:
        print(f"ERROR: Ocurrió un error durante la ejecución: {e}")
    finally:
        if engine:
            engine.dispose()
            print("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    main()