# -*- coding: utf-8 -*-
"""
PROYECTO:             EMPRESAS Y NEGOCIOS
AUTOR:                HITSS BI - JORGE MOSQUERA
OPERACION:            SCRIPT EN PYTHON CON LA ETL PARA EL PBI TRAICO CAVS
VERSION:              V_1.0
FECHA:                31/07/2025
DESCRIPCION:          SE CREA SCRIPT CON LAS EXTRACCIONES, TRANSFORMACIONES Y CARGUES DE DATA PARA EL PROCESO PBI TRAFICO CAVS
"""
# Librerias estandar
import sys
import pandas as pd
import logging
import os
from datetime import datetime
from sqlalchemy import text
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from sqlalchemy.exc import SQLAlchemyError
# Importacion de modulos personalizados y configuracion de rutas
import parametros as prm
ruta_modulos = prm.ruta_modulos["ruta"]
ruta_modulos_1 = prm.ruta_modulos["rut_cl"]
ruta_modulos_2 = prm.ruta_modulos["rut_con_sql"]
# Itera en cada ruta y la guarda en l avariable correspondiente
for ruta in [ruta_modulos, ruta_modulos_1, ruta_modulos_2]:
    if ruta not in sys.path:
        sys.path.append(ruta)
# Importacion de clases y funciones personalizadas
from config import conIntelienciaComercial as cdbi
#from Class_read_csv import ProcesadorBase
from pbi_trafico_cavs import query_fija,delete_query,query_movil,query_asig_cav

# Permite ver el todos los campos de un df
pd.set_option('display.max_columns', None)

# Configuracion del logger para registrar mensajes informativos y errores
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Clase principal del proceso
class pbitraficcavs:
     # Metodo que permite descargar los datos desde PostgreSQL
    def obtener_datos_postgres(self, query: str,engine) -> pd.DataFrame:
        try:
            df_bd_result = pd.read_sql(query,engine)
            return df_bd_result
        except Exception as e:
                print('Error al cextraer los datos de la bd: ',e)

    # Metodo que permite cargar los datos a PostgreSQL
    def cargar_df_a_tabla(self, df_tabla, name, schema, conexion):
        try:
            df_tabla.to_sql(
                name=name,
                con=conexion,
                schema=schema,
                if_exists='append',
                index=False
            )
            # print(f"Cargue exitoso a {schema}.{name}")
        except SQLAlchemyError as e:
            print(f"Error al cargar los datos: {e}")
            raise
    # Metodo que permite eliminar registros del mes antes de cargar los nuevos registros a PostgreSQL
    def eliminar_mes_de_tabla(self, df_tabla, name, schema, conexion):
        # Obtener el nombre del mes y el anio como valores individuales
        mes_nombre = df_tabla.mes.iloc[0]
        anio = int(df_tabla.anio.iloc[0])
        print(f"Eliminando registros del mes {mes_nombre} y anio {anio}")
        with conexion.connect() as conn:
            query = text(delete_query(schema, name))  # Usar la función externa
            conn.execute(query, {"mes_nombre": f"{mes_nombre}%", "anio": anio})
            conn.commit()

    # metodo de tranfromacion para calculo de campos
    def procesar_fechas(self, df):
        # Convertir FECHA_ESTADO a datetime
        df['fecha_estado'] = pd.to_datetime(df['fecha_estado'], format='%d/%m/%Y')
        # Crear la columna 'fecha' con el primer día del mes
        df['fecha'] = df['fecha_estado'].apply(lambda x: x.replace(day=1))
        # Crear la columna 'fec_carga' con la fecha actual del sistema
        df['fec_carga'] = datetime.now().date()
        return df

    # metodo para tranformacion cavs
    def agregar_campos_fecha(self,df):
        fecha_actual = datetime.now().date()
        meses_es_dict = prm.meses_es_dict
        df['fecha'] = df['fecha'] = fecha_actual.replace(day=1)
        df['anio'] = fecha_actual.strftime('%y')
        df['mes'] = meses_es_dict[fecha_actual.month]
        df['llave_cvc_mes_anio'] = df['cvc'] + '-' + df['mes'] + '-' + df['anio']
        df['fec_carga'] = fecha_actual.strftime('%Y-%m-%d')
        df = df[list(prm.cavs_org.keys())]
        return df



# Ejecuta una consulta SQL en una base de datos PostgreSQL y devuelve los resultados como un DataFrame.
    
######################### Funcion principal MAIN orquesta la ejecucion del proceso ETL#######################

if __name__ == '__main__':
    try:
        print("Inicio proceso:")
        # Obtener la fecha actual del sistema
        fecha_actual = datetime.now().date()
        pbi_tcavs = pbitraficcavs()
               
        #### Extraccion proceso fija ####
        df_resultado = pbi_tcavs.obtener_datos_postgres(query_fija(),cdbi().pg_ic_connect())
        print(f"la cantidad de regitros extraidos de fija: {len(df_resultado)}")
        # calcula nuevos campos
        df_resultado_f= pbi_tcavs.procesar_fechas(df_resultado)
        # elimina data exitente del mismo periodo 
        pbi_tcavs.eliminar_mes_de_tabla(df_tabla=df_resultado_f, name=prm.tablas_eschema["fij_mov"], schema=prm.tablas_eschema["eschema"], conexion=cdbi().pg_ic_connect())
        # carga datos del proceso fija
        pbi_tcavs.cargar_df_a_tabla(df_tabla=df_resultado_f, name=prm.tablas_eschema["fij_mov"], schema=prm.tablas_eschema["eschema"], conexion=cdbi().pg_ic_connect())
        
        #### Extraccion proceso movil ####
        df_resultado_m = pbi_tcavs.obtener_datos_postgres(query_movil(),cdbi().pg_ic_connect())
        print(f"la cantidad de regitros extraidos de movil: {len(df_resultado_m)}")
        # transforma campo tipo con valores esperados
        df_resultado_m['tipo'] = df_resultado_m['tipo'].replace({'ALTAS_M': 'ALTAS','PREPOS_M': 'PREPOS'})
        # calcula nuevos campos
        df_resultado_m= pbi_tcavs.procesar_fechas(df_resultado_m)
        # carga datos del proceso movil
        pbi_tcavs.cargar_df_a_tabla(df_tabla=df_resultado_m, name=prm.tablas_eschema["fij_mov"], schema=prm.tablas_eschema["eschema"], conexion=cdbi().pg_ic_connect())
      
        #### Extracion cavs ####
        df_resultado_c = pbi_tcavs.obtener_datos_postgres(query_asig_cav(),cdbi().pg_ic_connect())
        print(f"la cantidad de regitros extraidos de planta-cavs: {len(df_resultado_c)}")
        # calculamos campos de fecha requeridos
        df_resultado_c= pbi_tcavs.agregar_campos_fecha(df_resultado_c)
        # elimina data exitente del mismo periodo 
        pbi_tcavs.eliminar_mes_de_tabla(df_tabla=df_resultado_c, name=prm.tablas_eschema["asig_cav"], schema=prm.tablas_eschema["eschema"], conexion=cdbi().pg_ic_connect())
        # carga datos del proceso planta cavs
        pbi_tcavs.cargar_df_a_tabla(df_tabla=df_resultado_c, name=prm.tablas_eschema["asig_cav"], schema=prm.tablas_eschema["eschema"], conexion=cdbi().pg_ic_connect())
                
    except Exception as e:
        logger.error("Error general del proceso", exc_info=True)

