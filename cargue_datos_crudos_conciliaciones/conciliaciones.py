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
sys.path.append('C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\desarrollo_produccion')
#C:\\Users\\46196682\\.conda\\envs\\Empresas-Negocios-webscraping\\Parametros
import parametros_produccion as par
pd.set_option('display.max_columns', None)
# Ruta al archivo Excel
ruta_excel = r'C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\fuentes\\base_conciliaciones\\Conciliaciones _MB.xlsx'

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
destino = 'Conciliaciones'
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
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_produccion}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_resumen_cargue'
        
        df_resumen_cargue.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
    
    except SQLAlchemyError as e:
        fuentes.append('Conciliaciones')
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
    log_directory = par.ruta_log_produccion  # Usa la ruta definida en config.py
    log_file = os.path.join(log_directory, "conciliaciones_mb.log")

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
        
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_produccion}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'fuentes_cruda'
        nombre_tabla = 'tb_datos_crudos_conciliaciones_mb'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append('Conciliaciones')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatosBD.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

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
        
        conexion_errores = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_produccion}')
  
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_errores_cargue'
        errores.to_sql(nombre_tabla, con=conexion_errores, schema=nombre_esquema, if_exists='append', index=False)
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append('Conciliaciones')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def consultarHistoricoConciliaciones():
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
        engine = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_produccion}')

        #engine = conexion_BD()
        sql_consulta = "Select * \
                    from fuentes_cruda.tb_datos_crudos_conciliaciones_mb"
        df_historico_mb = pd.read_sql(sql_consulta, engine)
    
    
        return df_historico_mb
        
    except Exception as e:
        fuentes.append('Conciliaciones')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarHistoricoConciliaciones.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        engine.dispose()

# %%
def df_Conciliaciones(ruta_excel):


    try:
        # Leer el archivo Excel y especificar la hoja
        df_asignacion_bajas = pd.read_excel(ruta_excel, sheet_name='Paq_Datos_Marca_Blanca')
        
        # Verifica si la columna 'NIT' existe en el Excel
        if 'NIT' not in df_asignacion_bajas.columns:
            print("La columna 'NIT' no se encuentra en el archivo Excel.")
            return None
        df_asignacion_bajas.columns = [col.lower() for col in df_asignacion_bajas.columns]  # Convertir nombres de columnas a minúsculas
        
        
        return df_asignacion_bajas
    
    except Exception as e:
        fuentes.append('Conciliaciones')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(df_Conciliaciones.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
if __name__ == "__main__":
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
        fecha_base_excel = pd.to_datetime('1900-01-01')
        #df_ParametrosDesarrollo = consultarDatosParametrosDesarrollo()   
        #engine = conexion_BD()
        #resultado = consulta_mb(engine)
        #ejecutar_proceso(engine)
        #resultado_limpio = limpiar_data_mb(resultado) 

        
        df_conciliacionesHis = consultarHistoricoConciliaciones()
        df_conciliacionesHis['fecha_desact_paq'] = pd.to_datetime(
            df_conciliacionesHis['fecha_desact_paq'], errors='coerce'
        )
        df_conciliacionesHis['fecha_desac_paq_excel'] = (df_conciliacionesHis['fecha_desact_paq'] - fecha_base_excel).dt.days + 2  # Sumamos 2 por el ajuste de Excel
        df_conciliacionesHis['llaveDuplihis'] = df_conciliacionesHis['customer_id'].astype(str) + \
            df_conciliacionesHis['fecha_desac_paq_excel'].astype(str) + \
            df_conciliacionesHis['valor_paq'].astype(str) + \
            df_conciliacionesHis['desc_serv_instalado'].astype(str)


        df_conciliaciones_mb = df_Conciliaciones(ruta_excel)
        df_conciliaciones_mb['id_ejecucion'] = id_ejecucion  # Agregar la columna id_ejecucion con el mismo UUID en todas las fila
        df_conciliaciones_mb['id'] = [str(uuid.uuid4()) for _ in range(len(df_conciliaciones_mb))]  # Agregar la columna id con un UUID único por cada fila
        df_conciliaciones_mb['fecha_procesamiento'] = fecha_inicio  # Agregar la columna fecha_procesamiento con la fecha y hora actual
        df_conciliaciones_mb['id_estado'] = 1  # Crear el estado del registro como entero
        df_conciliaciones_mb['id_estado_registro'] = 1
        df_conciliaciones_mb.columns = [col.lower() for col in df_conciliaciones_mb.columns]  # Convertir los nombres de las columnas a minúsculas
        df_conciliaciones_mb['fecha_desact_paq'] = pd.to_datetime(
            df_conciliaciones_mb['fecha_desact_paq'], errors='coerce'
        )
        df_conciliaciones_mb['fecha_desac_paq_excel'] = (df_conciliaciones_mb['fecha_desact_paq'] - fecha_base_excel).dt.days + 2  # Sumamos 2 por el ajuste de Excel
        df_conciliaciones_mb['llaveDuplihis'] = df_conciliaciones_mb['customer_id'].astype(str) + \
            df_conciliaciones_mb['fecha_desac_paq_excel'].astype(str) + \
            df_conciliaciones_mb['valor_paq'].astype(str) + \
            df_conciliaciones_mb['desc_serv_instalado'].astype(str)
 

        df_concilicacionesfin_a = pd.merge(df_conciliaciones_mb, df_conciliacionesHis[['llaveDuplihis']], 
                    on=['llaveDuplihis'], 
                    how='left', 
                    indicator=True)
        
        df_concilicacionesfin = df_concilicacionesfin_a[df_concilicacionesfin_a['_merge'] == 'left_only'].drop(columns=['_merge'])
        

        df_conciliacionesHis.drop(columns=['llaveDuplihis', 'fecha_desac_paq_excel'], inplace=True)
        df_conciliaciones_mb.drop(columns=['llaveDuplihis', 'fecha_desac_paq_excel'], inplace=True)
        df_concilicacionesfin.drop(columns=['llaveDuplihis', 'fecha_desac_paq_excel'], inplace=True)

        registros = len(df_concilicacionesfin)
        cantidad_registros.append(registros)

        # Ejecucion cargue de datos ETL, se carga la funcion de CargueDatosBD, insercion a BD
        if registros > 0:
           df_resumen = cargueResumen(
        id_ejecucion, fecha_inicio_tr, fecha_fin_tr, duracion_proceso_seconds,
        'Conciliaciones_Mb', registros, 'tb_datos_crudos_conciliaciones_mb', id_estado
        )
        cargueDatosBD(df_concilicacionesfin)
  
    except Exception as e:
        fuentes.append('Conciliaciones')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()


