#############################################################################################################
#                                @AUTOR: LORENA HERNANDEZ                    	                        	#
#                                @PROCESO: PARAMETROS  DE CONEXIÓN - IMPORTE DE LIBRERIAS                   #                                                     									                        
#                                @DESCRIPCIÓN: POR BUENAS PRACTICAS PARA EL MANEJO DEL CODIDO SE PARAMETRIZA#
#                                EN ESTE ARCHIVO                                                            #
#############################################################################################################


# LIBRERIAS E IMPORTE

from sqlalchemy import create_engine  # Se utiliza para crear una conexión con la base de datos
from urllib.parse import quote  # Importa la función quote de urllib.parse para codificar URLs, especialmente contraseñas.
import pandas as pd  # Importa Pandas, una biblioteca de Python para la manipulación y análisis de datos.
import warnings  # Importa la biblioteca warnings para controlar las advertencias durante la ejecución del código.
import os  # Biblioteca para interactuar con el sistema operativo, como leer o escribir en el sistema de archivos.
import xlwings as xw  # Importa la librería xlwings para trabajar con archivos Excel.
import pyodbc
import pymysql

####### CONETARSE AL SERVIDOR POSTGRE - SQL

def database_connection(db_type, ip, port, user, password, bbdd):
    # Conexión para PostgreSQL
    if db_type.lower() == 'postgresql':
        connection_string = f'postgresql://{user}:{quote(password)}@{ip}:{port}/{bbdd}'
        sql = create_engine(connection_string)
        return sql
    # Conexión para SQL Server
    elif db_type.lower() == 'sqlserver':
        connection_string = f'mssql+pymssql://{user}:{quote(password)}@{ip}:{port}/{bbdd}'
        sql = create_engine(connection_string)
        return sql
    else:
        raise ValueError("Tipo de base de datos no soportado")

#######CARGAR A POSTGRE

def to_sql(dataframe, tableName, connection, type, index, chunksize):
    # Utiliza el contexto de la conexión para manejar la conexión de forma segura
    with connection.connect() as conn:
        dataframe.to_sql(name=tableName, con=conn, if_exists=type, index=index, chunksize=chunksize)



## FUNCIÓN DE EJECUCIÓN
## FUNCIÓN DE LECTURA
## PARAMS: SQL: QUERY A EJECUTAR
##         CONNECTION: VARIABLE DEPENDIENTE DE LA LIBRERIA SQLALCHEMY LA CUAL PERMITE LA 
##         COMUNICACIÓN CON EL SERVIDOR DE BASES DE DATOS
def executeQuery(sql, connection):
    # Utiliza el contexto de la conexión para ejecutar la consulta de forma segura
    with connection.connect() as conn:
        conn.execute(sql)
