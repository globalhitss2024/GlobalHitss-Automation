import pandas as pd
import logging
import numpy as np
import sys
import re
sys.path.append(r"C:\Users\46196682\Documents\Automatizacion\GlobalHitss-Automation")
from config.config import conIntelienciaComercial as connic
from config.config import conDbInteligenciaComercial as conndbi
from sql.ugis.bd_asignacion import UgiQueries as uq
from datetime import datetime, timedelta
from utils.umbral_utils import UmbralEvaluator

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

    def read_data_files(self, *files) -> pd.DataFrame:
        """
        Lee los archivos necesarios para el cruce de datos.
        """
        try:
            unificada_types = {'identificacion_1': str, 'des_tipo_cliente': str} 
            df_types = {'no_identificacion_final': str, 'des_segmento_comercial': str}
            
            df_unificada = pd.read_csv(f'{self.path}/{files[0]}', usecols=['identificacion_1', 'des_tipo_cliente'], 
                                       dtype=unificada_types, sep='|')
            
            df_fo_a = pd.read_csv(f'{self.path}/{files[1]}', usecols=['no_identificacion_final', 'des_segmento_comercial'],
                                  dtype=df_types, sep='|')

            self.logger.info("Limpiando columna 'identificacion_1' en df_unificada...")
            df_unificada['identificacion_1'] = df_unificada['identificacion_1'].apply(self.limpiar_documento)
            
            self.logger.info("Limpiando columna 'no_identificacion_final' en df_fo_a...")
            df_fo_a['no_identificacion_final'] = df_fo_a['no_identificacion_final'].apply(self.limpiar_documento)
            
            df_unificada.rename(columns={'identificacion_1': 'nit'}, inplace=True)
            df_fo_a.rename(columns={'no_identificacion_final': 'nit'}, inplace=True)
            
            return df_unificada, df_fo_a
        except Exception as e:
            print('Error durante la lectura de archivos: \n', e)

    def read_data_db(self, query: str, engine) -> pd.DataFrame:
        try:
            df_bd_result = pd.read_sql(query, engine)
            return df_bd_result
        except Exception as e:
            print('Ocurrió un error al intentar leer la BD de asignación: ', e)

    def cross_data_files(self, *df: pd.DataFrame) -> pd.DataFrame:
        """
        Cruza los DataFrames y devuelve un único archivo con el join.
        """
        try:
            df_unificada = df[0]
            df_fo = df[1]
            df_unificada['nit'] = df_unificada['nit'].astype('string')
            df_fo['nit'] = df_fo['nit'].astype('string')
            
            df_join = df_unificada.merge(df_fo, on='nit', how='inner')
            df_join['unificada'] = True
            df_join['fo'] = True

            unificado_join = df_unificada.merge(df_fo, on='nit', how='left', indicator=True)
            unificado_join = unificado_join[unificado_join['_merge'] == 'left_only']
            unificado_join['unificada'] = True
            unificado_join['fo'] = False

            fo_join = df_unificada.merge(df_fo, on='nit', how='right', indicator=True)
            fo_join = fo_join[fo_join['_merge'] == 'right_only']
            fo_join['unificada'] = False
            fo_join['fo'] = True
            
            df_final = pd.concat([df_join, unificado_join, fo_join], axis=0, ignore_index=True).drop(columns=['_merge'])
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
            df.to_sql('tb_base_final_convergente', engine_dev, schema='sch_integracion', if_exists='append', index=False)
            self.logger.info("Carga finalizada con éxito.")
        except Exception as e:
            print('Ocurrió un error al intentar cargar los datos: ', e)

if __name__ == '__main__':
    cd = generateUgis('C:\\Users\\46196682\\Documents\\EntradasConvergencia')
    df1, df2 = cd.read_data_files('UNIFICADA AGOSTO UMC.csv', 'FO 08.csv')
    df_bd_asignacion = cd.read_data_db(uq.query_asignacion(), connic().pg_ic_connect())
    df_bd_seg_homologo = cd.read_data_db(uq.query_seg_homologo(), engine_dev) 
    df_join_files = cd.cross_data_files(df1, df2)
    df_final = cd.cross_files_bd(df_join_files, df_bd_asignacion, df_bd_seg_homologo)

    #Eliminar duplicados generales    
    dups_exactos = df_final.duplicated(keep='first').sum()
    if dups_exactos > 0:
        df_final = df_final.drop_duplicates(keep='first').reset_index(drop=True)
        print(f"Filas duplicadas exactas eliminadas: {dups_exactos}")
    
    #unificacion duplicados por des_tipo
    df_final = cd.unificar_nits_duplicados(df_final)
    df_final.fillna({
    'segmento': 'Cliente potencial',
    'direccion': 'Cliente potencial'
    }, inplace=True)

    evaluator = UmbralEvaluator(df_final)
    df_final = evaluator.apply_rules()  


    if isinstance(df_final, pd.DataFrame) and not df_final.empty:
        print(df_final.info())
        print(df_final.head(10))
        cd.load_cross_to_db(df_final)
    else:
        print("El DataFrame final está vacío o no es válido.")
