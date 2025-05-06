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
ruta_excel = r'C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\fuentes\\base_ventas_cloud\\Ventas_cloud.xlsx'

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
destino = 'Ventas Manuales Cloud'
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
        
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_produccion}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_resumen_cargue'
        
        df_resumen_cargue.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
    
    except SQLAlchemyError as e:
        fuentes.append('ventas_manuales')
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
    log_file = os.path.join(log_directory, "ventas_manuales_cloud.log")

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
        nombre_tabla = 'tb_datos_crudos_ventas_manuales_cloud'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append('Ventas Manuales Cloud')
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
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_errores_cargue'
        errores.to_sql(nombre_tabla, con=conexion_errores, schema=nombre_esquema, if_exists='append', index=False)
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append('Ventas Manuales Cloud')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def consultarHistoricoVentas_cloud():
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
                    from fuentes_cruda.tb_datos_crudos_ventas_manuales_cloud"
        df_historico_mb = pd.read_sql(sql_consulta, engine)
    
    
        return df_historico_mb
        
    except Exception as e:
        fuentes.append('Ventas Manuales Cloud')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarHistoricoVentas_cloud.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        engine.dispose()

# %%
def df_Ventas_manuales_cloud(ruta_excel):
    """
    Funcion que se encarga de extraer los datos que vienen del archivo a cargar
    
    Argumentos:
        ruta_excel: archivo de excel para el cargue en la BD
    Retorna: 
        retorna el dataframe principal de datos a cargar ya limpiados
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        df_vtas_cloud_a = pd.read_excel(ruta_excel, sheet_name='base ventas Cloud')
        

        df_vtas_cloud_a.columns = [col.lower() for col in df_vtas_cloud_a.columns]  # Convertir nombres de columnas a minúsculas
        
        columnas_necesarias = [
        "fecha de venta", "mes", "año", "razon social", "nit",
        "customer id", "idsuscription", "producto", "no servicios",
        "valor total iva incluido", "nombres", "gerencia", 
        "jefatura", "cordinación", "segmento de alta", "operacion"
        ]
        
        df_filtrado_cloud = df_vtas_cloud_a[columnas_necesarias]
        df_filtrado_cloud.columns = df_filtrado_cloud.columns.str.replace(" ", "_").str.lower()
        return df_filtrado_cloud
    
    except Exception as e:
        fuentes.append('Ventas Manuales Cloud')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(df_Ventas_manuales_cloud.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
if __name__ == "__main__":
    """
    Programa principal que se encarga de controlar el orden en que se debe ejecutar el procesamiento 
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
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
        fecha_base_excel = pd.to_datetime('1900-01-01')
        #df_ParametrosDesarrollo = consultarDatosParametrosDesarrollo()   
        #engine = conexion_BD()
        #resultado = consulta_mb(engine)
        #ejecutar_proceso(engine)
        #resultado_limpio = limpiar_data_mb(resultado) 

        
        df_ventasmanualescloudHis = consultarHistoricoVentas_cloud()
        df_ventasmanualescloudHis['fecha_de_venta'] = pd.to_datetime(
            df_ventasmanualescloudHis['fecha_de_venta'], errors='coerce'
        )
        df_ventasmanualescloudHis['fecha_edit'] = (df_ventasmanualescloudHis['fecha_de_venta'] - fecha_base_excel).dt.days + 2  # Sumamos 2 por el ajuste de Excel
        df_ventasmanualescloudHis['llaveDuplihis'] = df_ventasmanualescloudHis['nit'].astype(str) + \
            df_ventasmanualescloudHis['fecha_edit'].astype(str) + \
            df_ventasmanualescloudHis['customer_id'].astype(str) + \
            df_ventasmanualescloudHis['producto'].astype(str)


        df_ventas_manualescloudf = df_Ventas_manuales_cloud(ruta_excel)
        df_ventas_manualescloudf['id_ejecucion'] = id_ejecucion  # Agregar la columna id_ejecucion con el mismo UUID en todas las fila
        df_ventas_manualescloudf['id'] = [str(uuid.uuid4()) for _ in range(len(df_ventas_manualescloudf))]  # Agregar la columna id con un UUID único por cada fila
        df_ventas_manualescloudf['fecha_procesamiento'] = fecha_inicio  # Agregar la columna fecha_procesamiento con la fecha y hora actual
        df_ventas_manualescloudf['id_estado_registro'] = 1
        df_ventas_manualescloudf.columns = [col.lower() for col in df_ventas_manualescloudf.columns]  # Convertir los nombres de las columnas a minúsculas
        df_ventas_manualescloudf['fecha_de_venta'] = pd.to_datetime(
            df_ventas_manualescloudf['fecha_de_venta'], errors='coerce'
        )
        df_ventas_manualescloudf['fecha_edit'] = (df_ventas_manualescloudf['fecha_de_venta'] - fecha_base_excel).dt.days + 2  # Sumamos 2 por el ajuste de Excel
        df_ventas_manualescloudf['llaveDuplihis'] = df_ventas_manualescloudf['nit'].astype(str) + \
            df_ventas_manualescloudf['fecha_edit'].astype(str) + \
            df_ventas_manualescloudf['customer_id'].astype(str) + \
            df_ventas_manualescloudf['producto'].astype(str)
 

        df_ventasmanuales_a = pd.merge(df_ventas_manualescloudf, df_ventasmanualescloudHis[['llaveDuplihis']], 
                    on=['llaveDuplihis'], 
                    how='left', 
                    indicator=True)
        
        df_ventasmanualesfin = df_ventasmanuales_a[df_ventasmanuales_a['_merge'] == 'left_only'].drop(columns=['_merge'])

        df_ventasmanualesfin.drop(columns=['llaveDuplihis', 'fecha_edit'], inplace=True)

        registros = len(df_ventasmanualesfin)
        cantidad_registros.append(registros)

        # Ejecucion cargue de datos ETL, se carga la funcion de CargueDatosBD, insercion a BD
        if registros > 0:
           df_resumen = cargueResumen(
        id_ejecucion, fecha_inicio_tr, fecha_fin_tr, duracion_proceso_seconds,
        'Ventas Manuales Cloud', registros, 'tb_datos_crudos_ventas_manuales_cloud', id_estado
        )
        cargueDatosBD(df_ventasmanualesfin)
  
    except Exception as e:
        fuentes.append('Ventas Manuales Cloud')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()


