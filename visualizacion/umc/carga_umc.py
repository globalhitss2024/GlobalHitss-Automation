'''                                                                                                                 
PROYECTO:               ANALÍTICA DE EMPRESAS Y NEGOCIOS                                                               
FRENTE DE TRABAJO:      VISUALIZACIÓN                                                                        
AUTOR:                  HITSS - FERNANDA ZAMBRANO                                                                     
OPERACIÓN:              SCRIPT DE CARGA DE TABLA                                                 
VERSIÓN:                v_1.0                                                                                           
FECHA:                  22/08/2025                                                                                      
DESCRIPCIÓN:            SCRIPT QUE PERMITE LA CARGA DE LA TABLA tb_emp_neg_umc
''' 

import os
import sys
import pandas as pd
from sqlalchemy import text

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
    excel_file_path = os.path.join(project_root, 'visualizacion', 'Base UMC.xlsx')

    # Variables de configuración
    db_schema = 'public'
    db_table = 'tb_emp_neg_umc'
    excel_sheet = 'Base'
    excel_range = 'A:L' # Columnas A hasta L, todas las filas
    
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
        print(f"tabla '{db_schema}.{db_table}' truncada.")
        
        # Instanciar excel_extractor con el archivo y rango directamente
        extractor = excel_extractor(archivo=excel_file_path, hoja_rango=f'{excel_sheet}!{excel_range}')
        df_excel = extractor.obtener_datos()  # Ya no necesita id_base
        df_excel = df_excel.dropna(how='all')  # Elimina filas donde todos los valores son nulos
        df_excel.columns = df_excel.columns.str.strip()  # Limpiar espacios en los nombres de columnas
        print(f"Se extrajeron {df_excel.shape[0]} filas del Excel.")

        # Carga de Datos en la Base de Datos
        loader = load_update_Datos(engine_conexion=engine)
        loader.cargar_df_a_tabla(df_tabla=df_excel, name=db_table, schema=db_schema)
        print(f"Carga Exitosa! Se insertaron {df_excel.shape[0]} registros.")

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