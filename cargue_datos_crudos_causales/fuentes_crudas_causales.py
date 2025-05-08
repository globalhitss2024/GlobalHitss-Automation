# %%
import pandas as pd
import openpyxl as opn
import psycopg2
from psycopg2 import sql, Error, OperationalError
import uuid
from datetime import datetime
import os
import shutil  # Para mover el archivo descargado
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import sys
import ast
sys.path.append('C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\desarrollo_notebook')
import parametros_desarrollo as par
pd.set_option('display.max_columns', None)
# Ruta al archivo Excel
ruta_excel_causales = r"C:\Users\46196682\OneDrive - Comunicacion Celular S.A.- Comcel S.A\BasesMantenimiento - bases\Causales.xlsx" ## dejar en parametros produccion

# %%
#VARIABLES GLOBALES
fecha_actual = datetime.today().date()
duracion = []
fuentes = []
cantidad_registros = []
estado = []
fecha_fin_procesamiento =[]
funcion_error = []
descripcion_error = []
id_ejecucion = str(uuid.uuid4())  # Generar UUID de ejecución
destino = 'Causales'
id_estado = '1'

# %%
"""
def salidaLogMonitoreo():
    
    Este metodo captura la informacion que se desea imprimir en el Log
    para monitoreo y funcionamiento del desarrollo
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        None
    
    Fecha_fin = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
    print(f"Fecha_inicio: {fecha_inicio}")
    print(f"Fecha_fin: {Fecha_fin}")
    print(f"Duracion: {duracion}")
    print(f"Fuentes: {fuentes}")
    print(f"Cantidad_registros: {cantidad_registros}")
    print(f"Destino: {destino}")
    print(f"Estado: {estado}")
    print("Lugar errores: ", ' | '.join(map(str, funcion_error)))
    print("Descripción errores: ", ' | '.join(map(str, descripcion_error)))
    if estado[0] == 1 :
        print("Ejecución exitosa")
    print("------------------------------------------------------------------")

"""
def salidaLogMonitoreo():
    """
    Este método captura la información que se desea imprimir en el Log
    para monitoreo y funcionamiento del desarrollo.
    """
    Fecha_fin = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
    logging.info(f"Fecha_inicio: {fecha_inicio}")
    logging.info(f"Fecha_fin: {Fecha_fin}")
    logging.info(f"Duracion: {duracion}")
    logging.info(f"Fuentes: {fuentes}")
    logging.info(f"Cantidad_registros: {cantidad_registros}")
    logging.info(f"Destino: {destino}")
    logging.info(f"Estado: {estado}")
    logging.info("Lugar errores: " + ' | '.join(map(str, funcion_error)))
    logging.info("Descripción errores: " + ' | '.join(map(str, descripcion_error)))
    if estado[0] == 1:
        logging.info("Ejecución exitosa")
    logging.info("------------------------------------------------------------------")

# %%
# Función para cargar resumen de datos en la BD
def cargueResumen(id_ejecucion, fecha_inicio_date,fecha_fin_procesamiento, duracion,fuentes, cantidad_registros, destino, id_estado):
    try:
        df_resumen_cargue = pd.DataFrame({
        'id_ejecucion': [id_ejecucion],  # Envolver en una lista
        'fecha_inicio_procesamiento': [fecha_inicio_date],
        'fecha_fin_procesamiento': [fecha_fin_procesamiento], 
        'duracion_segundos': [duracion],
        'fuentes': [fuentes],
        'cantidad_registros': [cantidad_registros],
        'destino': [destino],
        'id_estado': [id_estado],
    })
        Usuario_pro = 'postgres'
        contraseña_pro = '1Nt3l163nC14_C0m3rc14L'
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_resumen_cargue'
        
        df_resumen_cargue.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
    
    except SQLAlchemyError as e:
        fuentes.append('Causales')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueResumen.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %%
def configurarLogging():
    """
    Configura el logging para escribir en un archivo y en la salida estándar
    Utiliza la ruta definida en par.ruta_log para el directorio de logs.
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        None
    """
    # Configuración del logging
    log_directory = par.ruta_log  # Usa la ruta definida en config.py
    log_file = os.path.join(log_directory, "Causales.log")

    # Crear el directorio si no existe
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Configurar el logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode='a'),  # 'a' para modo append
            #logging.StreamHandler()  # Para imprimir en pantalla
        ]
    )

# %%
def insertarErroresDB():
    """
    Metodo para insertar a POSTGRESQL los errores capturados durante la ejecución
    Argumentos Globales:
        fecha_inicio: Captura la fecha en que inicio la ejecución
        fecha_fin: Captura la fecha en que finalizo la ejecución
        duracion: Duración del procesamiento
        fuente: Indica la fuente de donde provienen los datos
        cantidad_registros: Cantidad de registros por fuente
        destino: Indica la tabla a donde se estan ingestando los datos
        id_estado: Indica el estado del proceso definidos en la base de datos 
        funcion_error: Indica la función donde se esta presentando una falla
        descripcion_error: Descripción del error generado
    Retorna: 
        None
    Excepciones manejadas: 
        SQLAlchemyError as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        # Convertir las cadenas de texto a objetos datetime
        fecha_inicio_tr = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")
        fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_fin_tr = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")

        duracion_proceso_timedelta = fecha_fin_tr - fecha_inicio_tr
        duracion_proceso_seconds = duracion_proceso_timedelta.total_seconds()
        
        errores = pd.DataFrame({
            'fecha_inicio': fecha_inicio,
            'fecha_fin': fecha_fin,
            'duracion': duracion_proceso_seconds,
            'fuente': fuentes,
            'cantidad_registros': cantidad_registros,
            'destino': destino,
            'id_estado': estado,
            'funcion_error': funcion_error,
            'descripcion_error': descripcion_error
        })
        
        conexion_errores = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_errores_cargue'
        errores.to_sql(nombre_tabla, con=conexion_errores, schema=nombre_esquema, if_exists='append', index=False)
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append('Causales')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def cargueDatosBD(df_final):
    """
    Función que se encarga de cargar los dataframes procesados hacia la base de datos
    
    Argumentos:
        df_final: Contiene el dataframe que se requiere cargar a la BD
    Retorna: 
        None
    Excepciones manejadas: 
        SQLAlchemyError as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'fuentes_cruda'
        nombre_tabla = 'tb_datos_crudos_causales'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append('Causales')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatosBD.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %%
def consultarHistoricoCausales():
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_legalizadas
    
    Argumentos:
        None
    Retorna: 
        df_historico_mb : Retorna el historico de los datos cargados en la BD
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        engine = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial}')

        #engine = conexion_BD()
        sql_consulta = "Select * \
                    from fuentes_cruda.tb_datos_crudos_causales"
        df_historico_mb = pd.read_sql(sql_consulta, engine)
    
    
        return df_historico_mb
        
    except Exception as e:
        fuentes.append('Causales')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarHistoricoCausales.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        engine.dispose()

# %%
def df_Causales_f(ruta_excel_causales):
    """
    Función para leer y procesar la hoja 'Causales' desde un archivo Excel.

    Procesos:
        - Lee la hoja 'Causales' del archivo especificado.
        - Verifica si la columna 'RAZON_INICIAL' existe en los datos.
        - Limpia los nombres de las columnas, eliminando espacios y convirtiendo a minúsculas.
        - Elimina registros duplicados considerando las columnas clave.

    Argumentos:
        ruta_excel_causales (str): Ruta completa del archivo Excel que contiene la hoja 'Causales'.

    Retorna:
        pd.DataFrame: DataFrame con los datos únicos y columnas procesadas, o None si ocurre un error.

    Excepciones manejadas:
        Exception as e: Captura y registra cualquier error durante la lectura o procesamiento del archivo Excel.
    """



    try:
        # Leer el archivo Excel y especificar la hoja
        df_causales_a = pd.read_excel(ruta_excel_causales, sheet_name='Causales')
        
        # Verifica si la columna 'NIT' existe en el Excel
        if 'RAZON_INICIAL' not in df_causales_a.columns:
            print("La columna 'RAZON_INICIAL' no se encuentra en el archivo Excel.")
            return None
        df_causales_a.columns = [col.replace(" ", "_").lower() for col in df_causales_a.columns]  # Convertir nombres de columnas a minúsculas
        df_causales_a = df_causales_a.drop_duplicates(subset=['razon_inicial', 'tipo', 'tipo_v', 'tipo_g', 'td'])
        return df_causales_a
    
    except Exception as e:
        fuentes.append('Causales')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(df_Causales_f.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    
#df_causales = df_Causales_f(ruta_excel)
#df_causales.head(5)     

# %%
if __name__ == "__main__":

    
    """
    Bloque principal de ejecución del proceso ETL para la tabla 'Causales'.

    Funcionalidades:
        - Configura el logging.
        - Genera identificadores únicos para la ejecución.
        - Calcula la duración del proceso.
        - Consulta el histórico de registros existentes en la BD.
        - Lee los nuevos registros desde el archivo Excel.
        - Genera llaves únicas de comparación para evitar duplicados.
        - Compara y filtra los registros nuevos no existentes en la BD.
        - Agrega columnas de trazabilidad: id_ejecucion, id, fecha_procesamiento, id_estado_registro.
        - Carga los nuevos registros a la base de datos si existen.
        - Registra un resumen de la carga en la tabla correspondiente.
        - Maneja errores durante todo el proceso, registrándolos en la tabla de errores.

    Variables:
        id_ejecucion (str): UUID que identifica la ejecución.
        fecha_inicio (str): Fecha y hora de inicio del proceso.
        fecha_fin (str): Fecha y hora de fin del proceso.
        estado (int): Indicador de estado del proceso (1 = éxito, 2 = error).
        registros (int): Cantidad de registros nuevos a insertar.

    Excepciones manejadas:
        Exception as e: Captura cualquier error en el proceso y lo registra en la bitácora de errores.
    """
    
    try:
        configurarLogging()
        #Variables constantes dentro del codigo para funciones
        

        id_ejecucion = str(uuid.uuid4()).upper()  # Generar ID de ejecución
        fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_inicio_tr = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")
        fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_fin_tr = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
        id_estado = 1
        estado = 1  # O el valor adecuado para el estado
        duracion_proceso_timedelta = fecha_fin_tr - fecha_inicio_tr
        duracion_proceso_seconds = duracion_proceso_timedelta.total_seconds()
        

        
        df_CausalesHis = consultarHistoricoCausales()
        df_CausalesHis['llaveDuplihis'] = df_CausalesHis['razon_inicial'].astype(str) + \
            df_CausalesHis['tipo'].astype(str) + \
            df_CausalesHis['tipo_v'].astype(str) + \
            df_CausalesHis['tipo_g'].astype(str) + \
            df_CausalesHis['td'].astype(str)


        df_causales = df_Causales_f(ruta_excel_causales)
        df_causales['id_ejecucion'] = id_ejecucion  # Agregar la columna id_ejecucion con el mismo UUID en todas las fila
        df_causales['id'] = [str(uuid.uuid4()) for _ in range(len(df_causales))]  # Agregar la columna id con un UUID único por cada fila
        df_causales['fecha_procesamiento'] = fecha_inicio  # Agregar la columna fecha_procesamiento con la fecha y hora actual
        #df_causales['id_estado'] = 1  
        df_causales['id_estado_registro'] = 1
        
        df_causales['llaveDuplihis'] = df_causales['razon_inicial'].astype(str) + \
            df_causales['tipo'].astype(str) + \
            df_causales['tipo_v'].astype(str) + \
            df_causales['tipo_g'].astype(str) + \
            df_causales['td'].astype(str)
 

        df_Causalesfin_a = pd.merge(df_causales, df_CausalesHis[['llaveDuplihis']], 
                    on=['llaveDuplihis'], 
                    how='left', 
                    indicator=True)
        
        df_Causalesfin = df_Causalesfin_a[df_Causalesfin_a['_merge'] == 'left_only'].drop(columns=['_merge'])
        
        df_Causalesfin.drop(columns=['llaveDuplihis'], inplace=True)

        registros = len(df_Causalesfin)
        cantidad_registros.append(registros)

        # Ejecucion cargue de datos ETL, se carga la funcion de CargueDatosBD, insercion a BD
        if registros > 0:
           df_resumen = cargueResumen(
        id_ejecucion, fecha_inicio_tr, fecha_fin_tr, duracion_proceso_seconds,
        'Causales', registros, 'tb_datos_crudos_causales', id_estado
        )
        cargueDatosBD(df_Causalesfin)
  
    except Exception as e:
        fuentes.append('Causales')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()

# %%
"""
tb_datos_crudos_Causales
    
    razon_inicial VARCHAR(255),
    tipo VARCHAR(255), 
    tipo_v VARCHAR(255), 
    tipo_g VARCHAR(255),
    td VARCHAR(255),
    id_ejecucion VARCHAR(255),
    id VARCHAR(255) PRIMARY KEY,
    fecha_procesamiento VARCHAR(255),
    id_estado INT,
    id_estado_registro INT
"""    


