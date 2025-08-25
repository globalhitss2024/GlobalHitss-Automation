'''
PROYECTO:               ANALÍTICA DE EMPRESAS Y NEGOCIOS
FRENTE DE TRABAJO:      VISUALIZACIÓN
AUTOR:                  HITSS - FERNANDA ZAMBRANO
OPERACIÓN:              SCRIPT DE CARGA DE TABLA
VERSIÓN:                v_1.0
FECHA:                  25/08/2025
DESCRIPCIÓN:            SCRIPT QUE PERMITE LA CARGA DE LA TABLA tb_emp_metas_fijo_movil
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
from utils.Class_read_trsf_excel import ExcelExtractor, load_update_Datos

def main():
    """
    Proceso principal para cargar los datos desde Excel a la base de datos.
    """
    # Se construye la ruta relativa hacia el archivo Excel
    excel_file_path = os.path.join(project_root, 'visualizacion', 'Bases Empresas Fijo y Movil.xlsx')

    # Variables de configuración
    db_schema = 'public'
    db_table = 'tb_emp_metas_fijo_movil'
    excel_range = 'A:T' # Columnas A hasta T, todas las filas

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

        sheet_pattern = re.compile(r'^CUOTAS \d{4}$') # Expresión regular para filtrar las hojas que coinciden con "CUOTAS" seguido de 4 dígitos
        sheets_to_process = [sheet for sheet in all_sheet_names if sheet_pattern.match(sheet)]

        if not sheets_to_process:
            print("No se encontraron hojas de CUOTAS.")
            return
        
        # Iterar sobre las hojas filtradas para cargar datos
        for excel_sheet in sheets_to_process:
            print(f"Procesando hoja: {excel_sheet}")
            try:
                # Crear el DataFrame de configuración requerido por ExcelExtractor para extraer los datos
                config_df = pd.DataFrame([{
                    'id_base': 1,
                    'nombre_archivo_fuente': excel_file_path,
                    'rango': f'{excel_sheet}!{excel_range}'
                }])
        
                extractor = ExcelExtractor(df_base=config_df)
                df_excel, _ = extractor.obtener_datos(id_base=1) # Extraer datos del Excel
                df_excel.columns = df_excel.columns.str.strip() # Limpiar espacios en los nombres de columnas
                print(f"Se extrajeron {df_excel.shape[0]} filas del Excel.")

                # Mapeo de nombres de columnas de Excel a nombres de columnas de la base de datos
                column_mapping = {
                    'Cedula' : 'CEDULA',
                    'Gerencia/Jefatura' : 'JEFATURA',
                    'Gerencia' : 'GERENCIA',
                    'Dirección' : 'DIRECCION',
                    'Planta Comercial' : 'PLANTA_COMERCIAL',
                    'Cargo Actual' : 'CARGO_ACTUAL',
                    'Mes' : 'MES',
                    'Fecha' : 'FECHA',
                    'Reto Estratégico' : 'RETO_ESTRATEGICO',
                    'Convencional' : 'CONVENCIONAL',
                    'Multinacionales' : 'MULTINACIONALES',
                    'Meta Fijo' : 'META_FIJO',
                    'Neto Fijo Trimestral' : 'NETO_FIJO_TRIMESTRAL',
                    'Líneas Altas' : 'LINEAS_ALTAS',
                    'Altas Móvil' : 'ALTAS_MOVIL',
                    'Líneas Bajas' : 'LINEAS_BAJAS',
                    'Bajas Móvil' : 'BAJAS_MOVIL',
                    'Cambio De Plan' : 'CAMBIO_PLAN',
                    'Neto Móvil' : 'NETO_MOVIL',
                    'Reto Estratégico Movil' : 'RETO_MOVIL'
                }

                # Renombrar las columnas del DataFrame
                df_excel = df_excel.rename(columns=column_mapping)

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