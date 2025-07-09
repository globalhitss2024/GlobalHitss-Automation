import os
import json
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import logging
import pandas as pd

with open('.\config\params.json') as j:
    PARAMS = json.load(j)

class connections:
    """
    This class create the connect for different databases, only return the string connection
    for a database engine
    **params : is a key value params for connect to database
    type_connect: is a type of connection, database is for connect to database
    """
    def __init__(self, type_connect:str,**params):
        logging.basicConfig(format='%(acstime)s : %(levelname)s : %(message)s')
        self.logger = logging.getLogger()
        if type_connect =='database':
            self.user = params['user']
            self.psw = params['password']
            self.host = params['host']
            self.port = params['port']
            self.db = params['database']

    def get_postgres_connect(self):
        """return connect for postgres"""
        try:
           self.logger.info('Connecting to postgres')
           return create_engine(f"postgresql+psycopg2://{self.user}:{self.psw}@{self.host}/{self.db}", poolclass=NullPool)
        except Exception as e:
            print("An error has occurred when you try to connect to postgres: /n",e)

    def get_sqlserver_connect(self):
        """Return connect for SQL Server"""
        try:
            return f"mssql+pyodbc://{self.user}:{self.psw}@{self.host}/{self.db}?driver=ODBC+Driver+17+for+SQL+Server"
        except Exception as e:
            print("An error has occurred when you try to connect to sql server: /n",e)

class connItelienciaComercial(connections):
    """
    This class is for create a specific connection to inteligencia comercial database
    """
    def __init__(self):
        logging.basicConfig(format='%(acstime)s : %(levelname)s : %(message)s')
        self.logger = logging.getLogger()
        self.pg_param = PARAMS["inteligencia_comercial"]
        super().__init__('database', **self.pg_param[0])

    def pg_ic_connect(self):
        self.logger.info('Connecting to database...')
        try:
            conn = super().get_postgres_connect()
            return conn
        except Exception as e:
            self.logger.warning('Error to connect database Inteligencia comercial')


"""
example
if __name__ == '__main__':
    IC_connect = connItelienciaComercial().pg_ic_connect()
    df = pd.read_sql("select * from proc_genericos.tb_aux_importacion_bases",IC_connect)
    print(df.head(5))
"""


