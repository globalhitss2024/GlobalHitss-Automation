import multiprocessing  ## Permite la creación de procesos, ofrece concurrencia local como hilos.
import os               ## Proporciona funciones para interactuar con el sistema operativo.
import time             ## Proporciona funciones para trabajar con tiempos y fechas.
import pandas as pd     ## Biblioteca de análisis de datos, permite manipular y analizar datos.

from datetime import date ## Manejo de fechas y tiempo.
from datetime import datetime ## Manejo de fechas y tiempo.
from datetime import timedelta ## Manejo de fechas y tiempo.

import locale           ## Proporciona funciones de localización y regionalización.
from Conect import * ## Importa funciones o variables definidas en el módulo sqlConnect.
import pydoc            ## Proporciona funciones para acceder a la documentación de Python.

from concurrent.futures import ThreadPoolExecutor ## Permiten la ejecución concurrente de código.
from concurrent.futures import ProcessPoolExecutor ## Permiten la ejecución concurrente de código.

import numpy as np      ## Biblioteca para manejar arrays y matrices y hacer operaciones matemáticas básicas y avanzadas.
import dask.dataframe as dd  ## Paralelización y manejo de dataframes grandes que no caben en memoria.
import logging          ## Proporciona funciones para registrar los eventos mientras se ejecuta el código.
import json             ## Proporciona funciones para trabajar con objetos JSON.
import inspect          ## Proporciona varias funciones para obtener información sobre objetos en vivo.
from functools import wraps  ## Proporciona un decorador para actualizar la función decorada, útil para "decorar" funciones.





# Configura el nivel de log global (DEBUG, INFO, WARNING, ERROR, CRITICAL)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Crea una instancia del logger
logger = logging.getLogger(__name__)





# Función para calcular el tiempo de ejecución de una operación
def calcular_tiempo(proceso):
    def decorator(func):
        def wrapper(*args, **kwargs):
            
            with open(r'var/json/conManager.json', 'r') as file:
                dataCon = json.load(file)


            ip_destino, port_destino, user_destino, password_destino, bbdd_destino = (
                dataCon[3]["ip"],
                dataCon[3]["port"],
                dataCon[3]["user"],
                dataCon[3]["password"],
                dataCon[3]["bbdd"]
            )

            try:
                dfOrigen = func(*args, **kwargs)
                cantidad_registros = len(dfOrigen)
            except:
                pass
            engine = mysql_connection(ip_destino, port_destino, user_destino, password_destino, bbdd_destino)
            connection_destino = engine.connect()
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                end_time = time.time()
                execution_time = end_time - start_time
                mensaje = f"ERROR EXEC {proceso} {func.__name__}: {str(e)}"
                tiempo = f"{execution_time:.6f}"
                logger.error(mensaje)
                # Registrar el log en la tabla con el nombre del proceso
                log_data = {
                    'timestamp': pd.Timestamp.now(),
                    'nivel': 'EXEC',
                    'status': 'ERROR',
                    'mensaje': mensaje,
                    'tiempo_ejecucion':tiempo,
                    'proceso': proceso
                }
                with engine.connect() as conn:
                    conn.execute("INSERT INTO tb_auditoria_procesos_python (timestamp, nivel, status, mensaje, tiempo_ejecucion, proceso) VALUES (%(timestamp)s, %(nivel)s, %(status)s, %(mensaje)s, %(tiempo_ejecucion)s, %(proceso)s)", log_data)
                raise
            else:
                end_time = time.time()
                execution_time = end_time - start_time
                mensaje = f"SUCCESS EXEC {proceso} {func.__name__}"
                tiempo = f"{execution_time:.6f}"
                logger.debug(mensaje)
                # Registrar el log en la tabla con el nombre del proceso
                log_data = {
                    'timestamp': pd.Timestamp.now(),
                    'nivel': 'EXEC',
                    'status': 'SUCCESS',
                    'mensaje': mensaje,
                    'total_data': cantidad_registros,
                    'tiempo_ejecucion':tiempo,
                    'proceso': proceso
                }
                with engine.connect() as conn:
                    conn.execute("INSERT INTO tb_auditoria_procesos_python (timestamp, nivel, status, mensaje, total_rows, tiempo_ejecucion, proceso) VALUES (%(timestamp)s, %(nivel)s, %(status)s, %(mensaje)s, %(total_data)s, %(tiempo_ejecucion)s, %(proceso)s)", log_data)
                return result
        return wrapper
    return decorator



def get_num_cores():
    return multiprocessing.cpu_count()

def get_num_threads():
    return os.cpu_count()

def read_sql_file(path):
    # Leer el archivo SQL
    with open(path, 'r') as file:
        sql_create = file.read()
    
    return sql_create