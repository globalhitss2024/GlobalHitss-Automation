# %%
"""
PROYECTO:             EMPRESAS Y NEGOCIOS
AUTOR:                HITSS BI - GEORGE GALINDO
OPERACION:            SCRIPT PARA CARGUE DE TABLA DE CAVS
VERSION:              V_1.0
FECHA:                28/07/2025
DESCRIPCION:          SE CREA SCRIPT CON LAS EXTRACCIONES DE neg_fijo.tb_ventas_nf     
"""

# %%
import pandas as pd
import os
import re
import sys
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine,text
import logging
import sys
import os
sys.path.append(r"C:\Users\46196682\Documents\Automatizacion\GlobalHitss-Automation")
from config.config import conIntelienciaComercial, conDbInteligenciaComercialdev ### tener pendiente para cambiar bd a produccion, introducir bd de desarrollo
from sql.cavs.ConsultasCavs import consulta_cavs
from utils.Class_read_trsf_excel import load_update_Datos

if __name__ == "__main__":
    conexion = conIntelienciaComercial().pg_ic_connect()
    engine = conDbInteligenciaComercialdev().pg_ic_connect()

    try:
        df = pd.read_sql(consulta_cavs, conexion)
        print("Conexión exitosa, datos cargados")
        print(df.head(5))

        # Cargar el Dg
        cargador = load_update_Datos(engine_conexion=engine)
        cargador.cargar_df_a_tabla(df_tabla=df, name="tb_cavs_generadas", schema="proc_genericos")

    except Exception as e:
        print("Error al ejecutar la carga ", e)

        # Intentar rollback manual si la transacción quedó abierta
        try:
            connection = engine.raw_connection()
            if connection.in_transaction():
                connection.rollback()
                print("Rollback ejecutado desde el script principal.")
            connection.close()
        except Exception as rb_error:
            print("Error durante rollback ", rb_error)

"""
# %%
if __name__ == "__main__":
    # Crea la conexion a la BD
    conexion = conIntelienciaComercial().pg_ic_connect()
    engine = conDbInteligenciaComercialdev().pg_ic_connect()
    try:
        # Ejecutar la consulta
        df = pd.read_sql(consulta_cavs, conexion)
        print("Conexión exitosa. Datos cargados:")
        print( df.head(5))

        # Cargar DataFrame
        cargador = load_update_Datos(engine_conexion=engine)
        cargador.cargar_df_a_tabla(df_tabla=df, name="tb_cavs_generadas", schema="proc_genericos")
    except Exception as e:
        print("Error al ejecutar la consulta o conectar a la base de datos:")
        print(e)
"""

