import pandas as pd
import logging
import numpy as np
import sys
import re
import os
sys.path.append(r"C:\Users\46196682\Documents\Automatizacion\GlobalHitss-Automation")
from config.config import conIntelienciaComercial as connic
from config.config import conDbInteligenciaComercial as conndbi
from sql.ugis.bd_asignacion import UgiQueries as uq
from datetime import datetime, timedelta

# --- Configuración de la base de datos ---
dbi = conndbi()
dbi.db = 'DBInteligenciaComercialDesarrollo'
engine_dev = dbi.pg_ic_connect()

current_datetime = datetime.now()
time_delta = current_datetime - timedelta(days=30)
formatted_datetime = time_delta.strftime("%Y-%m-01")
print(formatted_datetime)

class generateUgis:
    """
    Esta clase tiene el objetivo de cruzar datos y crear la base de datos final para UGIs.
    """
    def __init__(self, path:str):
        """
        Constructor de la clase.
        self.path: Ruta de la carpeta donde se encuentran los archivos.
        """
        logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
        self.logger = logging.getLogger()
        self.path = path

    @staticmethod
    def limpiar_documento(documento):
        """
        Limpia un número de documento según reglas específicas.
        """
        if not isinstance(documento, str):
            return None
        
        if re.match(r'^\d{2}-', documento):
            partes = documento.split('-')
            return f"{partes[0]}-{partes[1]}"
        else:
            return documento.split('-')[0]

    def read_data_files(self, unificada_file: str, fo_file: str, marco_file: str) -> pd.DataFrame:
        """
        Lee los tres archivos necesarios para el cruce de datos.
        """
        try:
            # --- Lectura de UNIFICADA ---
            unificada_types = {'identificacion': str, 'des_tipo_cliente': str} 
            df_unificada = pd.read_csv(f'{self.path}/{unificada_file}', usecols=['identificacion', 'des_tipo_cliente'], 
                                       dtype=unificada_types, sep=';')
            
            # --- Lectura de FO ---
            df_types = {'no_identificacion_final': str, 'des_segmento_comercial': str}
            df_fo_a = pd.read_csv(f'{self.path}/{fo_file}', usecols=['no_identificacion_final', 'des_segmento_comercial'],
                                  dtype=df_types, sep='|')

            # --- Lectura de MARCO ---
            self.logger.info(f"Leyendo el archivo {marco_file}...")
            columnas_marco = ['nit', 'segmento', 'descrp1', 'descrp2']
            # Se lee el archivo sin encabezado y se asignan los nombres de columna
            df_marco = pd.read_csv(f'{self.path}/{marco_file}', sep=';', names=columnas_marco)
            # Nos quedamos solo con la columna 'nit' que es la necesaria
            df_marco = df_marco[['nit']].copy()
            df_marco['nit'] = df_marco['nit'].astype(str)

            # --- Limpieza y Renombrado ---
            self.logger.info("Limpiando columna 'identificacion' en df_unificada...")
            df_unificada['identificacion'] = df_unificada['identificacion'].apply(self.limpiar_documento)
            df_unificada.rename(columns={'identificacion': 'nit'}, inplace=True)
            
            self.logger.info("Limpiando columna 'no_identificacion_final' en df_fo_a...")
            df_fo_a['no_identificacion_final'] = df_fo_a['no_identificacion_final'].apply(self.limpiar_documento)
            df_fo_a.rename(columns={'no_identificacion_final': 'nit'}, inplace=True)

            self.logger.info("Limpiando columna 'nit' en df_marco...")
            df_marco['nit'] = df_marco['nit'].apply(self.limpiar_documento)
            
            # Devolvemos los tres DataFrames
            return df_unificada, df_fo_a, df_marco
        
        except Exception as e:
            print('Error durante la lectura de archivos: \n', e)
            return None, None, None # Devolver None en caso de error

    def read_data_db(self, query: str, engine) -> pd.DataFrame:
        try:
            df_bd_result = pd.read_sql(query, engine)
            return df_bd_result
        except Exception as e:
            print('Ocurrió un error al intentar leer la BD de asignación: ', e)

    def cross_data_files(self, df_unificada: pd.DataFrame, df_fo: pd.DataFrame, df_marco: pd.DataFrame) -> pd.DataFrame:
        """
        Cruza los tres DataFrames iniciales usando un full outer join y devuelve un único archivo.
        """
        try:
            self.logger.info("Iniciando cruce de los 3 archivos base...")
            # Asegurar que la clave 'nit' sea de tipo string para evitar errores en el merge
            df_unificada['nit'] = df_unificada['nit'].astype(str)
            df_fo['nit'] = df_fo['nit'].astype(str)
            df_marco['nit'] = df_marco['nit'].astype(str)

            df_marco['marco'] = True
            
            # 1. Cruzar Unificada con FO (outer join para no perder ningún NIT)
            df_merged = pd.merge(df_unificada, df_fo, on='nit', how='outer')
            
            # 2. Cruzar el resultado anterior con Marco (outer join)
            df_final = pd.merge(df_merged, df_marco, on='nit', how='outer')
            
            # 3. Crear las columnas booleanas para identificar el origen de cada NIT
            # Un NIT vino de 'unificada' si su columna 'des_tipo_cliente' no es nula
            df_final['unificada'] = df_final['des_tipo_cliente'].notna()
            # Un NIT vino de 'fo' si su columna 'des_segmento_comercial' no es nula
            df_final['fo'] = df_final['des_segmento_comercial'].notna()
            # La columna 'marco' ya existe, solo rellenamos los NaN con False
            df_final['marco'].fillna(False, inplace=True)
            
            self.logger.info("Cruce de archivos base finalizado.")
            return df_final
        except Exception as e:
            print('Ocurrió un error al intentar cruzar los datos: \n', e)
    
    def cross_files_bd(self, *df) -> pd.DataFrame:
        try:
            df_cross = df[0].merge(df[1], on='nit', how='left')
            df_cross['fec_carga'] = formatted_datetime
            df_cross = df_cross.merge(df[2], left_on='segmento_actual', right_on='nombre_asignacion', how='left')
            df_cross.drop(columns=['nit_a', 'nombre_asignacion'], inplace=True, errors='ignore')
            return df_cross
        except Exception as e:
            print('Ocurrió un error al cruzar archivos con la BD de asignación: \n', e)
    
    def unificar_nits_duplicados(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Unifica los registros con el mismo NIT que tienen diferente 'des_tipo_cliente'.
        """
        self.logger.info("Iniciando unificación de NITs duplicados...")
        
        # Marcar duplicados por NIT con la nueva columna 'duplicated'
        df['duplicated'] = df.duplicated(subset='nit', keep=False)
        df_sin_duplicados = df[~df['duplicated']].copy()

        df_duplicados = df[df['duplicated']].copy()
        
        if not df_duplicados.empty:
            # duplicados tomando el primer registro y cambiar campo des_tipo_cliente
            df_unificados = df_duplicados.drop_duplicates(subset='nit', keep='first').copy()
            df_unificados['des_tipo_cliente'] = 'ClienteFijaMovil'
            
            # concatenar df con duplicados unificados y ya unificados
            df_final = pd.concat([df_sin_duplicados, df_unificados], ignore_index=True)
            self.logger.info(f"Se unificaron {len(df_duplicados) - len(df_unificados)} registros duplicados.")
            return df_final
        else:
            self.logger.info("No se encontraron NITs duplicados para unificar.")
            return df

    def load_cross_to_db(self, df: pd.DataFrame):
        try:
            self.logger.info("Cargando datos a la base de datos...")
            df.to_sql('tb_base_final_convergente_marco', engine_dev, schema='sch_integracion', if_exists='append', index=False)
            self.logger.info("Carga finalizada con éxito.")
        except Exception as e:
            print('Ocurrió un error al intentar cargar los datos: ', e)

## ------------------- MODIFICADO ------------------- ##
if __name__ == '__main__':
    cd = generateUgis('C:\\Users\\46196682\\Documents\\EntradasConvergencia')
    output_folder = cd.path
    output_filename = 'resultado_convergencia.csv'
    full_output_path = os.path.join(output_folder, output_filename)
    # Ahora se leen los 3 archivos
    df1, df2, df3 = cd.read_data_files('UNIFICADA JULIO UMC.csv', 'fo julio.csv', 'MARCO_JUL25.txt')
    
    # Se continúa con el flujo normal si los archivos se leyeron correctamente
    if df1 is not None and df2 is not None and df3 is not None:
        df_bd_asignacion = cd.read_data_db(uq.query_asignacion(), connic().pg_ic_connect())
        df_bd_seg_homologo = cd.read_data_db(uq.query_seg_homologo(), engine_dev) 
        
        # Se cruzan los 3 DataFrames iniciales
        df_join_files = cd.cross_data_files(df1, df2, df3)
        
        # El resto del proceso sigue igual
        df_final = cd.cross_files_bd(df_join_files, df_bd_asignacion, df_bd_seg_homologo)

        #Eliminar duplicados generales    
        dups_exactos = df_final.duplicated(keep='first').sum()
        if dups_exactos > 0:
            df_final = df_final.drop_duplicates(keep='first').reset_index(drop=True)
            print(f"Filas duplicadas exactas eliminadas: {dups_exactos}")
        
        #unificacion duplicados por des_tipo
        df_final = cd.unificar_nits_duplicados(df_final)

        if isinstance(df_final, pd.DataFrame) and not df_final.empty:
            print("Información del DataFrame final:")
            print(df_final.info())
            print("\nPrimeras 10 filas:")
            print(df_final.head(10))
            print("\nVerificación de las nuevas columnas de cruce:")
            print(df_final[['nit', 'unificada', 'fo', 'marco']].head(10))
            df_final.to_csv(full_output_path, index=False, sep=';', encoding='utf-8-sig')
            cd.load_cross_to_db(df_final)
        else:
            print("El DataFrame final está vacío o no es válido.")
    else:
        print("No se pudo continuar el proceso debido a un error en la lectura de archivos.")