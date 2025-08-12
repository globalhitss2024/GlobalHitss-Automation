import pandas as pd
import logging
import numpy as np
import sys
sys.path.append('C:/Users/lopezcrc/automatizacion/GlobalHitss-Automation')
from config.config import conIntelienciaComercial as connic
from config.config import conDbInteligenciaComercial as conndbi
from sql.ugis.bd_asignacion import UgiQueries as uq
from datetime import datetime, timedelta
current_datetime = datetime.now()
time_delta = current_datetime - timedelta(days=30)
formatted_datetime = time_delta.strftime("%Y-%m-01")
print(formatted_datetime)

class generateUgis:
    """
    This class has to objetive to cross data and create final database for ugis
    """
    def __init__(self, path:str):
        """
        This fuction is a constructor for class
        self.path : this param has the folder path where files is.
        """
        logging.basicConfig(format='%(acstime)s : %(levelname)s : %(message)s')
        self.logger = logging.getLogger()
        self.path = path

    def read_data_files(self, *files) -> pd.DataFrame:
        """
        This method is for read the necessary files for cross data
        *files : is a args list with the names of files, important the files should be the extension example: myfile.csv
                 the first file names should be a unificada and second should be the piramide y marco
        """
        try:
            unificada_types = {'identificacion':str,'des_tipo_cliente':str} 
            piram_marco_types = {'no_identificacion_final':str,'des_operacion_conjunta':str,'flag':str}
            df_unificada = pd.read_csv(f'{self.path}/{files[0]}', usecols=['identificacion','des_tipo_cliente'], 
                                    dtype=unificada_types,sep=';')
            df_piram_marco = pd.read_csv(f'{self.path}/{files[1]}', usecols=['no_identificacion_final','des_operacion_conjunta','flag'],
                                        dtype=piram_marco_types,
                                        sep='|')
            df_unificada.rename(columns={'identificacion':'nit'},inplace=True)
            df_piram_marco.rename(columns={'no_identificacion_final':'nit'},inplace=True)
            return df_unificada, df_piram_marco
        except Exception as e:
            print('Error during reading files: \n', e)

    def read_data_db(self, query: str,engine) -> pd.DataFrame:
        try:
            df_bd_result = pd.read_sql(query,engine)
            return df_bd_result
        except Exception as e:
            print('An error has occurred when you trying to read bd asignacion: ',e)

    def cross_data_files(self, *df: pd.DataFrame) -> pd.DataFrame:
        """
        this method cross the files and return only file the join between to files piramide and unificada
        *df: this is an args list param, recive the dataframe in order first df_unificada, second df_piram_marco
        """
        try:
            df_unificada = df[0]
            df_piram_marco = df[1]
            df_unificada['nit'] = df_unificada['nit'].astype('string')
            df_piram_marco['nit'] = df_piram_marco['nit'].astype('string')
            df_join = df_unificada.merge(df_piram_marco, left_on='nit', right_on='nit',how='inner')
            df_join['unificada'] = True
            df_join['piramide'] = True

            unficado_join = df_unificada.merge(df_piram_marco, left_on='nit', right_on='nit',how='left')
            unficado_join = unficado_join[unficado_join['flag'].isnull()]
            unficado_join['unificada'] = True
            unficado_join['piramide'] = False

            piram_join = df_unificada.merge(df_piram_marco, left_on='nit', right_on='nit',how='right')
            piram_join = piram_join[piram_join['des_tipo_cliente'].isnull()]
            piram_join['unificada'] = False
            piram_join['piramide'] = True
            # united bases
            df_final = pd.concat([df_join,unficado_join,piram_join],axis=0,ignore_index=True)
            return df_final
        except Exception as e:
            print('An error has occurred when trying to cross data: \n',e)

    def cross_files_bd(self,*df) -> pd.DataFrame:
        try:
            df_cross = df[0].merge(df[1], left_on = 'nit', right_on = 'nit', how = 'left')
            #df_cross['cliente_potencial'] = np.where(df_cross['nit_a'].isnull(), True, False)
            df_cross['fec_carga'] = formatted_datetime
            df_cross = df_cross.merge(df[2], left_on = 'segmento_actual', right_on = 'nombre_asignacion', how = 'left')
            df_cross.drop(columns=['nit_a','nombre_asignacion'], inplace=True)
            return df_cross
        except Exception as e:
            print('An error has occurred when trying to cross data files and bd asignacion: \n',e)
    
    def load_cross_to_db(self,df: pd.DataFrame):
        try:
            df.to_sql('tbl_base_final_ugis_old',conndbi().pg_ic_connect(),schema='sch_integracion',if_exists='append',index=False)
        except Exception as e:
            print('An error has ocurred when you trying to load data: ',e)

if __name__ == '__main__':
    cd = generateUgis('C:\\Users\\lopezcrc\\Documents\\EyN\\Fuentes\\entrada')
    df1,df2 = cd.read_data_files('UNIFICADA JULIO UMC.csv','BD JULIO P&M.csv')
    df_bd_asignacion = cd.read_data_db(uq.query_asignacion(),connic().pg_ic_connect())
    df_bd_seg_homologo = cd.read_data_db(uq.query_seg_homologo(),conndbi().pg_ic_connect()) 
    df_join_files = cd.cross_data_files(df1,df2)
    df_final = cd.cross_files_bd(df_join_files,df_bd_asignacion,df_bd_seg_homologo)
    print(df_final.columns)
    cd.load_cross_to_db(df_final)