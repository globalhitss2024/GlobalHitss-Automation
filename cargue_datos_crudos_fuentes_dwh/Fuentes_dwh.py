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
sys.path.append('C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\desarrollo_produccion')
import parametros_produccion as par
pd.set_option('display.max_columns', None)



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
destino = 'Fuentes DWH'
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
        fuentes.append('Fuentes DWH')
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
    log_file = os.path.join(log_directory, "Fuentes DWH.log")

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
        
        conexion_errores = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_produccion}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_errores_cargue'
        errores.to_sql(nombre_tabla, con=conexion_errores, schema=nombre_esquema, if_exists='append', index=False)
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append('Fuente DWH')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def consultarHistoricoBD_Dim_agentes():
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
                    from fuentes_cruda.tb_datos_crudos_dwh_dim_agentes"
        df_historico_dim_agentes = pd.read_sql(sql_consulta, engine)
    
    
        return df_historico_dim_agentes
        
    except Exception as e:
        fuentes.append('Fuentes DWH')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarHistoricoBD_Dim_agentes.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        engine.dispose()

# %%
def consultarHistoricoBD_Plan_negocios():
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
                    from fuentes_cruda.tb_datos_crudos_dwh_plan_negocios"
        df_historico_Plan_negocios = pd.read_sql(sql_consulta, engine)
    
    
        return df_historico_Plan_negocios
        
    except Exception as e:
        fuentes.append('Fuentes DWH')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarHistoricoBD_Plan_negocios.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        engine.dispose()

# %%
def consultarHistoricoBD_Ventas():
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
                    from fuentes_cruda.tb_datos_crudos_dwh_ventas"
        df_historico_Ventas = pd.read_sql(sql_consulta, engine)
    
    
        return df_historico_Ventas
        
    except Exception as e:
        fuentes.append('Fuentes DWH')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarHistoricoBD_Ventas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        engine.dispose()

# %%
def consultarHistoricoBD_Cp():
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
                    from fuentes_cruda.tb_datos_crudos_dwh_cambio_plan"
        df_historico_cp = pd.read_sql(sql_consulta, engine)
    
    
        return df_historico_cp
        
    except Exception as e:
        fuentes.append('Fuentes DWH')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarHistoricoBD_Cp.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        engine.dispose()

# %%
def consultarHistoricoBD_Base_seg():
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
                    from fuentes_cruda.tb_datos_crudos_dwh_base_seg"
        df_historico_base_seg = pd.read_sql(sql_consulta, engine)
    
    
        return df_historico_base_seg
        
    except Exception as e:
        fuentes.append('Fuentes DWH')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarHistoricoBD_Base_seg.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        engine.dispose()

# %%
def consultarHistoricoBD_Codigo_Ciudad():
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
                    from fuentes_cruda.tb_datos_crudos_dwh_codigo_ciudad"
        df_historico_codigo_ciudad = pd.read_sql(sql_consulta, engine)
    
    
        return df_historico_codigo_ciudad
        
    except Exception as e:
        fuentes.append('Fuentes DWH')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarHistoricoBD_Codigo_Ciudad.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        engine.dispose()

# %%
def cargueDatosBD_Dim_agentes(df_final):
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
        nombre_tabla = 'tb_datos_crudos_dwh_dim_agentes'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append('Dhw dim agentes')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatosBD_Dim_agentes.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %%
def cargueDatosBD_Plan_negocios(df_final):
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
        nombre_tabla = 'tb_datos_crudos_dwh_plan_negocios'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append('Dwh Plan negocio')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatosBD_Plan_negocios.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %%
def cargueDatosBD_Ventas(df_final):
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
        nombre_tabla = 'tb_datos_crudos_dwh_ventas'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append('Dwh Ventas')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatosBD_Ventas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %%
def cargueDatosBD_Cp(df_final):
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
        nombre_tabla = 'tb_datos_crudos_dwh_cambio_plan'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append('Dwh Ventas')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatosBD_Cp.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %%
def cargueDatos_Base_seg(df_final):
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
        nombre_tabla = 'tb_datos_crudos_dwh_base_seg'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append('Dwh Ventas')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatos_Base_seg.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %%
def cargueDatos_Codigo_Ciudad(df_final):
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
        nombre_tabla = 'tb_datos_crudos_dwh_codigo_ciudad'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append('Dwh Ventas')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatos_Codigo_Ciudad.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %%
def conexion_BD():
    try:
        connection = psycopg2.connect(
            dbname="dwh_db",    
            user="45110947",            
            password="Mmilu28()*",    
            host="100.123.59.140",          
            port="5432"                
        )
        print("Conexión a la base de datos Yellowbrick exitosa.")
        return connection
    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# %%
def consulta_Dim_agentes(engine):
    try:
        # Consulta tabla DWH_DB.SEGMENTACION.INH_DIM_AGENTES
        consulta = """
         SELECT
        COD_AGENTE,COST_ID_AGENTE,CUSTCODE_AGENTE,NAME_AGENTE,ESTADO_AGENTE,
        CANAL as CANAL_COMERCIAL,SUBCANAL,CATEGORIA,REGION_COMERCIAL AS DIRECCION,GERENCIA,
        CIUDAD_AGENTE,DEPARTAMENTO,REGION_CONCESION,
        UNIDAD
        FROM DWH_DB.SEGMENTACION.inh_dim_agentes
        """
        
        print(f"Ejecutando consulta: {consulta}")  # Imprime la consulta para depuración
        cursor = engine.cursor()
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        # Obtener los nombres de las columnas
        columnas = [desc[0] for desc in cursor.description]

        # Crear el DataFrame con los resultados
        df_dim_agentes = pd.DataFrame(resultados, columns=columnas)

        # Verificar si el DataFrame está vacío
        if df_dim_agentes.empty:
            print("No se encontraron resultados.")
        else:
            df_dim_agentes.head(9)  # Mostrar las primeras 5 filas

        return df_dim_agentes

    except Exception as e:
        fuentes.append('Fuentes DHW')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consulta_Dim_agentes.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    



# %%
def consulta_Plan_negocios(engine):
    try:
        # Consulta tabla DWH_DB.SEGMENTACION.INH_DIM_AGENTES
        consulta = """
         select * from DWH_DB.SEGMENTACION.INH_DIM_PLANES t
        """
        
        print(f"Ejecutando consulta: {consulta}")  # Imprime la consulta para depuración
        cursor = engine.cursor()
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        # Obtener los nombres de las columnas
        columnas = [desc[0] for desc in cursor.description]

        # Crear el DataFrame con los resultados
        df_plan_negocios = pd.DataFrame(resultados, columns=columnas)

        # Verificar si el DataFrame está vacío
        if df_plan_negocios.empty:
            print("No se encontraron resultados.")
        else:
            df_plan_negocios.head(9)  # Mostrar las primeras 5 filas

        return df_plan_negocios

    except Exception as e:
        fuentes.append('Fuentes DHW')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consulta_Plan_negocios.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()



# %%
def consulta_Cp(engine):
    try:
        # Consulta tabla DWH_DB.SEGMENTACION.INH_DIM_AGENTES
        consulta = """
         select A.CO_ID
,('NEG_PLANES') as FLAG
,A.CUSTOMER_ID
,A.TELE_NUMB
,I.PRODUCTO AS TIPO_LINEA
,H.CD_SEGMENTO_CLIENTE AS CODIGO_SEGMENTO_CLIENTE
,D.CUSTCODE_AGENTE
,H.DS_SEGMENTO_CLIENTE AS SEGMENTACION
,H.DS_UNIDAD_NEGOCIO
,E.REGION_DESC AS REGION_GEOGRAFICA
,D.REGION_COMERCIAL
,D.GERENCIA
,D.CANAL
,D.COD_AGENTE
,TO_CHAR(cast(A.FE_ACTIVACION as DATE),'DD/MM/YYYY') FE_ACTIVACION
,G.COD_CICLO
,A.NO_DIA_CORTE_FACTURACION
,A.NO_MESES_ANTIGUEDAD
,M.DS_RANGO_ANTIGUEDAD
,I.COD_PLAN AS COD_PLAN_ORIGEN
,I.PLAN_DESC AS DS_PLAN_ORIGEN
,cast (I.VLR_CFM as numeric) AS CFM_PLAN_ORIGEN
,I.TIPO_PLAN AS TIPO_PLAN_ORIGEN
,N.DS_GAMA_PLAN AS DS_GAMA_PLAN_ORIGEN
,J.COD_PLAN AS COD_PLAN_DESTINO
,J.PLAN_DESC AS DS_PLAN_DESTINO
,pn.ban_pyme as PLAN_NEGOCIOS
,cast (J.VLR_CFM as numeric) AS CFM_PLAN_DESTINO
,J.TIPO_PLAN AS TIPO_PLAN_DESTINO
,O.DS_GAMA_PLAN AS DS_GAMA_PLAN_DESTINO
,cast (A.VL_CFM_NETO as numeric)
,TO_CHAR(cast(A.FE_CREACION as DATE),'DD/MM/YYYY') FE_CREACION
,A.NO_TICKLER
,TO_CHAR(cast(A.FE_CAMBIO as DATE),'DD/MM/YYYY') FE_CAMBIO
,L.DS_TIPO_GRADE
,P.CD_USUARIO_RED AS CD_USUARIO_RED_USUARIO_CRECION
,P.DS_NOMBRE_USUARIO AS DS_NOMBRE_USUARIO_CREACION
,P.DS_UBICACION_USUARIO AS DS_UBICACION_USUARIO_CREACION
,P.DS_CANAL_USUARIO AS DS_CANAL_USUARIO_CREACION
,CASE WHEN A.NO_EJECUTADO=1 then 'Cerrado' else 'Abierto' END ESTADO_TICKLER
,TO_CHAR(cast(A.FE_CIERRE as DATE),'DD/MM/YYYY') FE_CIERRE
,Q.CD_USUARIO_RED AS CD_USUARIO_RED_USUARIO_CIERRE
,Q.DS_NOMBRE_USUARIO AS DS_NOMBRE_USUARIO_CIERRE
,Q.DS_UBICACION_USUARIO AS DS_UBICACION_USUARIO_CIERRE
,Q.DS_CANAL_USUARIO AS DS_CANAL_USUARIO_CIERRE
,A.DS_TICKLER
,A.DS_NOMBRE_CLIENTE
,F.TIPO_IDENTIFICADOR_DESC
,A.DS_IDENTIFICACION
,T.IDENTIFICACION
,T.IDENTIFICACION_MTR
,A.DS_TELEFONO_1
,A.DS_TELEFONO_2
,A.IMEI
,A.IMSI
,cast (A.VL_CFM_NETO_CONTABLE_VOZ as numeric)
,cast (A.VL_CFM_NETO_CONTABLE_DATOS as numeric)
FROM DWH_DB.CLIENTES.TBL_FACT_CLIEN_CAMBIOS_PLAN_EJECUTADOS A
LEFT JOIN (SELECT CUSTCODE_AGENTE,REGION_COMERCIAL,GERENCIA,CANAL,COD_AGENTE FROM DWH_DB.SEGMENTACION.INH_DIM_AGENTES) D ON A.COD_AGENTE=D.COD_AGENTE
LEFT JOIN (SELECT REGION_DESC,COD_MERCADO FROM DWH_DB.SEGMENTACION.INH_DIM_REGIONES) E ON A.COD_MERCADO=E.COD_MERCADO
LEFT JOIN (SELECT TIPO_IDENTIFICADOR_DESC,TIPO_IDENTIFICACION FROM DWH_DB.SEGMENTACION.INH_DIM_TIPO_IDENTIFICADOR) F ON A.TIPO_IDENTIFICACION=F.TIPO_IDENTIFICACION
LEFT JOIN (SELECT COD_CICLO FROM DWH_DB.SEGMENTACION.INH_DIM_CICLOS) G ON A.COD_CICLO=G.COD_CICLO
LEFT JOIN (SELECT CD_SEGMENTO_CLIENTE, DS_SEGMENTO_CLIENTE, DS_UNIDAD_NEGOCIO,SK_SEGMENTO_CLIENTE FROM DWH_DB.CLIENTES.TBL_DIM_SEGMENTO_CLIENTE_T1) H ON A.SK_SEGMENTO_CLIENTE=H.SK_SEGMENTO_CLIENTE
LEFT JOIN (SELECT COD_PLAN,PLAN_DESC,VLR_CFM,TIPO_PLAN,PRODUCTO FROM DWH_DB.SEGMENTACION.INH_DIM_PLANES) I ON A.COD_PLAN_ORIGEN=I.COD_PLAN
LEFT JOIN (SELECT COD_PLAN,PLAN_DESC,VLR_CFM,TIPO_PLAN,PRODUCTO FROM DWH_DB.SEGMENTACION.INH_DIM_PLANES) J ON A.COD_PLAN_DESTINO=J.COD_PLAN
LEFT JOIN (SELECT DS_TIPO_GRADE,SK_TIPO_GRADE FROM DWH_DB.CLIENTES.TBL_DIM_TIPO_GRADE_T0) L ON A.SK_TIPO_GRADE=L.SK_TIPO_GRADE
LEFT JOIN (SELECT DS_RANGO_ANTIGUEDAD,SK_RANGO_ANTIGUEDAD FROM DWH_DB.CLIENTES.TBL_DIM_RANGO_ANTIGUEDAD_T1) M ON A.SK_RANGO_ANTIGUEDAD=M.SK_RANGO_ANTIGUEDAD
LEFT JOIN (SELECT DS_GAMA_PLAN,SK_GAMA_PLAN FROM DWH_DB.CLIENTES.TBL_DIM_GAMA_PLAN_T1) N ON A.SK_GAMA_PLAN_ORIGEN=N.SK_GAMA_PLAN
LEFT JOIN (SELECT DS_GAMA_PLAN,SK_GAMA_PLAN FROM DWH_DB.CLIENTES.TBL_DIM_GAMA_PLAN_T1) O ON A.SK_GAMA_PLAN_DESTINO=O.SK_GAMA_PLAN
LEFT JOIN (SELECT CD_USUARIO_RED,DS_NOMBRE_USUARIO,DS_UBICACION_USUARIO,DS_CANAL_USUARIO,SK_USUARIO_UBICACION FROM DWH_DB.CLIENTES.TBL_DIM_USUARIO_UBICACION_T2) P ON A.SK_USUARIO_CREACION=P.SK_USUARIO_UBICACION
LEFT JOIN (SELECT CD_USUARIO_RED,DS_NOMBRE_USUARIO,DS_UBICACION_USUARIO,DS_CANAL_USUARIO,SK_USUARIO_UBICACION FROM DWH_DB.CLIENTES.TBL_DIM_USUARIO_UBICACION_T2) Q ON A.SK_USUARIO_CIERRE=Q.SK_USUARIO_UBICACION
LEFT JOIN (SELECT CO_ID, IDENTIFICACION, IDENTIFICACION_MTR FROM SEGMENTACION.INH_SEG_BSCS_CLIENTES ) T ON A.CO_ID =T.CO_ID
LEFT JOIN (select BAN_PYME, COD_PLAN from DWH_DB.SEGMENTACION.INH_DIM_PLANES) PN on J.COD_PLAN = PN.cod_plan
WHERE cast(A.FE_CREACION as DATE) >= '2024-04-15'
and pn.ban_pyme = 'S'
        """
        
        print(f"Ejecutando consulta: {consulta}")  # Imprime la consulta para depuración
        cursor = engine.cursor()
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        # Obtener los nombres de las columnas
        columnas = [desc[0] for desc in cursor.description]

        # Crear el DataFrame con los resultados
        df_cp = pd.DataFrame(resultados, columns=columnas)

        # Verificar si el DataFrame está vacío
        if df_cp.empty:
            print("No se encontraron resultados.")
        else:
            df_cp.head(9)  # Mostrar las primeras 5 filas

        return df_cp

    except Exception as e:
        fuentes.append('Fuentes DHW')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consulta_Cp.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    

# %%
def consulta_Ventas(engine):
    try:
        # Consulta tabla DWH_DB.SEGMENTACION.INH_DIM_AGENTES
        consulta = """
         SELECT
            ('NEGOCIOS') AS Segmento,
            R5.segmento AS FLAG,
            M.CLASECAUSALINIC,
            M.CO_ID,
            M.TELE_NUMB,
            M.PLAN,
            M.PAQUETE,
            M.TIPO,
            M.AGENT_CODE,
            translate(M.RAZON_INICIAL, 'áéíóú', 'aeiou') AS RAZON_INICIAL,
            translate(M.RAZON_FINAL, 'áéíóú', 'aeiou') AS RAZON_FINAL,
            M.FECHA_ULTEST,
            M.ACTIVACION,
            M.SSN,
            M.TMCYSPC,
            M.CIUDAD,
            CAST(M.CFM AS INTEGER),
            M.AGENT_NAME,
            M.DOCUMENTO,
            SUBSTRING(ciclo, POSITION(' - ' IN ciclo) + 2, 3) AS ciclo,
            M.identificacion_venderdor,
            M.CUSTCODE
        FROM 
            DWH.BSCS_ACTIVACIONES M
        JOIN 
            DWH_DB.VENTAS.TBL_VENT_SEG_INF_CONFIDENCIALES R5 ON M.SSN = R5.identificacion
        WHERE 
            (M.RAZON_INICIAL LIKE '%32%' OR 
             M.RAZON_INICIAL LIKE '%57%' OR 
             M.RAZON_INICIAL LIKE '%267%' OR 
             M.RAZON_INICIAL LIKE '%345%' OR 
             M.RAZON_INICIAL LIKE '%362%' OR 
             M.RAZON_INICIAL LIKE '%118%' OR 
             M.RAZON_INICIAL LIKE '%285%' OR 
             M.RAZON_INICIAL LIKE '%500%' OR 
             M.RAZON_INICIAL LIKE '%202%')
        
        UNION

        SELECT
            ('NEGOCIOS') AS Segmento,
            R5.segmento AS FLAG,
            M.CLASECAUSALINIC,
            M.CO_ID,
            M.TELE_NUMB,
            M.PLAN,
            M.PAQUETE,
            M.TIPO,
            M.AGENT_CODE,
            translate(M.RAZON_INICIAL, 'áéíóú', 'aeiou') AS RAZON_INICIAL,
            translate(M.RAZON_FINAL, 'áéíóú', 'aeiou') AS RAZON_FINAL,
            M.FECHA_ULTEST,
            M.ACTIVACION,
            M.SSN,
            M.TMCYSPC,
            M.CIUDAD,
            CAST(M.CFM AS INTEGER),
            M.AGENT_NAME,
            M.DOCUMENTO,
            SUBSTRING(ciclo, POSITION(' - ' IN ciclo) + 2, 3) AS ciclo,
            M.identificacion_venderdor,
            M.CUSTCODE
        FROM 
            DWH.BSCS_ACTIVACIONES M
        JOIN 
            DWH_DB.VENTAS.TBL_VENT_SEG_INF_CONFIDENCIALES R5 ON M.SSN = R5.identificacion
        WHERE 
            (M.RAZON_INICIAL LIKE '%333%' OR 
             M.RAZON_INICIAL LIKE '%444%' OR 
             M.RAZON_INICIAL LIKE '%555%' OR 
             M.RAZON_INICIAL LIKE '%666%' OR 
             M.RAZON_INICIAL LIKE '%777%')
        """
        
        print(f"Ejecutando consulta: {consulta}")  # Imprime la consulta para depuración
        cursor = engine.cursor()
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        # Obtener los nombres de las columnas
        columnas = [desc[0] for desc in cursor.description]

        # Crear el DataFrame con los resultados
        df_ventas = pd.DataFrame(resultados, columns=columnas)

        # Verificar si el DataFrame está vacío
        if df_ventas.empty:
            print("No se encontraron resultados.")
        else:
            df_ventas.head(9)  # Mostrar las primeras 5 filas

        return df_ventas

    except Exception as e:
        fuentes.append('Fuentes DHW')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consulta_Ventas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()




# %%
def consulta_Base_seg(engine):
    try:
        # Consulta tabla DWH_DB.SEGMENTACION.INH_DIM_AGENTES
        consulta = """
         SELECT 
         x.identificacion  as CUST_ID,
        ('') as Cod_plan,
        x.segmento as Flag,
        ('') as Usuario
        FROM DWH_DB.VENTAS.TBL_VENT_SEG_INF_CONFIDENCIALES x
        WHERE CB_ESACTUAL='1' 
        """
        
        print(f"Ejecutando consulta: {consulta}")  # Imprime la consulta para depuración
        cursor = engine.cursor()
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        # Obtener los nombres de las columnas
        columnas = [desc[0] for desc in cursor.description]

        # Crear el DataFrame con los resultados
        df_base_seg = pd.DataFrame(resultados, columns=columnas)

        # Verificar si el DataFrame está vacío
        if df_base_seg.empty:
            print("No se encontraron resultados.")
        else:
            df_base_seg.head(9)  # Mostrar las primeras 5 filas

        return df_base_seg

    except Exception as e:
        fuentes.append('Fuentes DHW')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consulta_Base_seg.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consulta_Codigo_Ciudad(engine):
    try:
        # Consulta tabla DWH_DB.SEGMENTACION.INH_DIM_AGENTES
        consulta = """
          SELECT x.* FROM clientes.tbl_dim_distribuidor_bscs_t1 x
          where x.tipo_distribuidor in ('D','T','MAY','C')

        
        """
        
        print(f"Ejecutando consulta: {consulta}")  # Imprime la consulta para depuración
        cursor = engine.cursor()
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        # Obtener los nombres de las columnas
        columnas = [desc[0] for desc in cursor.description]

        # Crear el DataFrame con los resultados
        df_codigo_ciudad = pd.DataFrame(resultados, columns=columnas)

        # Verificar si el DataFrame está vacío
        if df_codigo_ciudad.empty:
            print("No se encontraron resultados.")
        else:
            df_codigo_ciudad.head(9)  # Mostrar las primeras 5 filas

        return df_codigo_ciudad

    except Exception as e:
        fuentes.append('Fuentes DHW')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consulta_Codigo_Ciudad.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()




# %%
if __name__ == "__main__":
    try:
        configurarLogging()
        #Variables constantes dentro del codigo para funciones
        


        id_ejecucion_dim_agente = str(uuid.uuid4()).upper()
        id_ejecucion_plan_negocios = str(uuid.uuid4()).upper()
        id_ejecucion_base_seg = str(uuid.uuid4()).upper()
        id_ejecucion_ventas = str(uuid.uuid4()).upper()
        id_ejecucion_codigo_ciudad = str(uuid.uuid4()).upper()
        id_ejecucion_cambioplan = str(uuid.uuid4()).upper()
        
        fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_inicio_tr = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")
        fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_fin_tr = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
        id_estado = 1
        estado = 1  # O el valor adecuado para el estado
        duracion_proceso_timedelta = fecha_fin_tr - fecha_inicio_tr
        duracion_proceso_seconds = duracion_proceso_timedelta.total_seconds()
        
        # Historicos de las tablas con la llave creada 
        engine = conexion_BD()
        df_his_dim_agente = consultarHistoricoBD_Dim_agentes()
        df_his_dim_agente['llaveDuplihis'] = df_his_dim_agente['cod_agente'].astype(str) + \
            df_his_dim_agente['cost_id_agente'].astype(str) + \
            df_his_dim_agente['custcode_agente'].astype(str) + \
            df_his_dim_agente['name_agente'].astype(str) + \
            df_his_dim_agente['categoria'].astype(str)


        df_his_plan_negocios = consultarHistoricoBD_Plan_negocios()
        df_his_plan_negocios['llaveDuplihis'] = df_his_plan_negocios['cod_plan'].astype(str) + \
            df_his_plan_negocios['vlr_cfm'].astype(str) + \
            df_his_plan_negocios['segm_cfm'].astype(str) + \
            df_his_plan_negocios['categoria'].astype(str)
        
        df_his_ventas = consultarHistoricoBD_Ventas()
        df_his_ventas['llaveDuplihis'] = df_his_ventas['segmento'].astype(str) + \
            df_his_ventas['co_id'].astype(str) + \
            df_his_ventas['tele_numb'].astype(str) + \
            df_his_ventas['paquete'].astype(str) + \
            df_his_ventas['fecha_ultest'].astype(str) + \
            df_his_ventas['custcode'].astype(str)  
        
        df_his_base_seg = consultarHistoricoBD_Base_seg()

        df_his_codigo_ciudad = consultarHistoricoBD_Codigo_Ciudad()
        df_his_codigo_ciudad['llaveDuplihis'] = df_his_codigo_ciudad['sk_distribuidor'].astype(str) + \
            df_his_codigo_ciudad['cod_distribuidor'].astype(str) + \
            df_his_codigo_ciudad['fecha_creacion'].astype(str)
        
        df_his_cambioplan = consultarHistoricoBD_Cp()
        df_his_cambioplan['llaveDuplihis'] = df_his_cambioplan['co_id'].astype(str) + \
            df_his_cambioplan['customer_id'].astype(str) + \
            df_his_cambioplan['tele_numb'].astype(str) + \
            df_his_cambioplan['cod_plan_origen'].astype(str) + \
            df_his_cambioplan['cod_plan_destino'].astype(str) + \
            df_his_cambioplan['fe_creacion'].astype(str) 

        #----------------------------------------------------------------------------------------------------------------------------------------------------

        df_dim_agente = consulta_Dim_agentes(engine) # ya esta tb_datos_crudos_dwh_dim_agentes
        df_dim_agente['id_ejecucion'] = id_ejecucion_dim_agente  # Agregar la columna id_ejecucion con el mismo UUID en todas las fila
        df_dim_agente['id'] = [str(uuid.uuid4()) for _ in range(len(df_dim_agente))]  # Agregar la columna id con un UUID único por cada fila
        df_dim_agente['fecha_procesamiento'] = fecha_inicio  # Agregar la columna fecha_procesamiento con la fecha y hora actual
        df_dim_agente['id_estado'] = 1  # Crear el estado del registro como entero
        df_dim_agente['id_estado_registro'] = 1

        df_dim_agente['llaveDuplihis'] = df_dim_agente['cod_agente'].astype(str) + \
            df_dim_agente['cost_id_agente'].astype(str) + \
            df_dim_agente['custcode_agente'].astype(str) + \
            df_dim_agente['name_agente'].astype(str) + \
            df_dim_agente['categoria'].astype(str)



        merge_df_dim_agente = pd.merge(df_dim_agente, df_his_dim_agente[['llaveDuplihis']], 
                    on=['llaveDuplihis'], 
                    how='left', 
                    indicator=True)
        df_dim_agente_fin = merge_df_dim_agente[merge_df_dim_agente['_merge'] == 'left_only'].drop(columns=['_merge'])

        registros_dim_agent = len(df_dim_agente_fin)
        df_dim_agente_fin.drop(columns=['llaveDuplihis'], inplace=True)

        if registros_dim_agent > 0:
           df_resumen = cargueResumen(
        id_ejecucion_dim_agente, fecha_inicio_tr, fecha_fin_tr, duracion_proceso_seconds,
        'Fuente DWH', registros_dim_agent, 'tb_datos_crudos_dwh_dim_agentes', id_estado
        )
        cargueDatosBD_Dim_agentes(df_dim_agente_fin)

        #----------------------------------------------------------------------------------------------------------------------------------------------------

        df_plan_negocios = consulta_Plan_negocios(engine) # ya esta tb_datos_crudos_dwh_plan_negocios
        df_plan_negocios['id_ejecucion'] = id_ejecucion_plan_negocios  # Agregar la columna id_ejecucion con el mismo UUID en todas las fila
        df_plan_negocios['id'] = [str(uuid.uuid4()) for _ in range(len(df_plan_negocios))]  # Agregar la columna id con un UUID único por cada fila
        df_plan_negocios['fecha_procesamiento'] = fecha_inicio  # Agregar la columna fecha_procesamiento con la fecha y hora actual
        df_plan_negocios['id_estado'] = 1  # Crear el estado del registro como entero
        df_plan_negocios['id_estado_registro'] = 1
     
        df_plan_negocios['llaveDuplihis'] = df_plan_negocios['cod_plan'].astype(str) + \
            df_plan_negocios['vlr_cfm'].astype(str) + \
            df_plan_negocios['segm_cfm'].astype(str) + \
            df_plan_negocios['categoria'].astype(str)
        merge_df_plan_negocios = pd.merge(df_plan_negocios, df_his_plan_negocios[['llaveDuplihis']], 
                    on=['llaveDuplihis'], 
                    how='left', 
                    indicator=True)
        df_plan_negocios_fin = merge_df_plan_negocios[merge_df_plan_negocios['_merge'] == 'left_only'].drop(columns=['_merge'])
        
        registros_plan_negocios = len(df_plan_negocios_fin)
        df_plan_negocios_fin.drop(columns=['llaveDuplihis'], inplace=True)
        
        if registros_plan_negocios > 0:
           df_resumen = cargueResumen(
        id_ejecucion_plan_negocios, fecha_inicio_tr, fecha_fin_tr, duracion_proceso_seconds,
        'Fuente DWH', registros_plan_negocios, 'tb_datos_crudos_dwh_plan_negocios', id_estado
        )
        cargueDatosBD_Plan_negocios(df_plan_negocios_fin)
        
        #----------------------------------------------------------------------------------------------------------------------------------------------------

        df_ventas = consulta_Ventas(engine) # ya esta tb_datos_crudos_dwh_ventas
        df_ventas['id_ejecucion'] = id_ejecucion_ventas  # Agregar la columna id_ejecucion con el mismo UUID en todas las fila
        df_ventas['id'] = [str(uuid.uuid4()) for _ in range(len(df_ventas))]  # Agregar la columna id con un UUID único por cada fila
        df_ventas['fecha_procesamiento'] = fecha_inicio  # Agregar la columna fecha_procesamiento con la fecha y hora actual
        df_ventas['id_estado'] = 1  # Crear el estado del registro como entero
        df_ventas['id_estado_registro'] = 1
        df_ventas['llaveDuplihis'] = df_ventas['segmento'].astype(str) + \
            df_ventas['co_id'].astype(str) + \
            df_ventas['tele_numb'].astype(str) + \
            df_ventas['paquete'].astype(str) + \
            df_ventas['fecha_ultest'].astype(str) + \
            df_ventas['custcode'].astype(str) 
        
        merge_df_ventas = pd.merge(df_ventas, df_his_ventas[['llaveDuplihis']], 
                    on=['llaveDuplihis'], 
                    how='left', 
                    indicator=True)
        df_ventas_fin = merge_df_ventas[merge_df_ventas['_merge'] == 'left_only'].drop(columns=['_merge'])

        registros_ventas = len(df_ventas_fin)
        df_ventas_fin.drop(columns=['llaveDuplihis'], inplace=True)
        
        if registros_ventas > 0:
           df_resumen = cargueResumen(
        id_ejecucion_ventas, fecha_inicio_tr, fecha_fin_tr, duracion_proceso_seconds,
        'Fuente DWH', registros_ventas, 'tb_datos_crudos_dwh_ventas', id_estado
        )
        cargueDatosBD_Ventas(df_ventas_fin)

        #----------------------------------------------------------------------------------------------------------------------------------------------------

        df_base_seg = consulta_Base_seg(engine) # ya esta tb_datos_crudos_dwh_base_seg
        df_base_seg['id_ejecucion'] = id_ejecucion_base_seg  # Agregar la columna id_ejecucion con el mismo UUID en todas las fila
        df_base_seg['id'] = [str(uuid.uuid4()) for _ in range(len(df_base_seg))]  # Agregar la columna id con un UUID único por cada fila
        df_base_seg['fecha_procesamiento'] = fecha_inicio  # Agregar la columna fecha_procesamiento con la fecha y hora actual
        df_base_seg['id_estado'] = 1  # Crear el estado del registro como entero
        df_base_seg['id_estado_registro'] = 1
        merge_df_base_seg = pd.merge(df_base_seg, df_his_base_seg[['cust_id']], 
                    on=['cust_id'], 
                    how='left', 
                    indicator=True)
        df_base_seg_fin = merge_df_base_seg[merge_df_base_seg['_merge'] == 'left_only'].drop(columns=['_merge'])

        registros_base_seg = len(df_base_seg_fin)


        if registros_base_seg > 0:
           df_resumen = cargueResumen(
        id_ejecucion_base_seg, fecha_inicio_tr, fecha_fin_tr, duracion_proceso_seconds,
        'Fuente DWH', registros_base_seg, 'tb_datos_crudos_dwh_base_seg', id_estado
        )
        cargueDatos_Base_seg(df_base_seg_fin)

        #----------------------------------------------------------------------------------------------------------------------------------------------------

        df_codigo_ciudad = consulta_Codigo_Ciudad(engine)# ya esta tb_datos_crudos_dwh_codigo_ciudad
        df_codigo_ciudad['id_ejecucion'] = id_ejecucion_codigo_ciudad  # Agregar la columna id_ejecucion con el mismo UUID en todas las fila
        df_codigo_ciudad['id'] = [str(uuid.uuid4()) for _ in range(len(df_codigo_ciudad))]  # Agregar la columna id con un UUID único por cada fila
        df_codigo_ciudad['fecha_procesamiento'] = fecha_inicio  # Agregar la columna fecha_procesamiento con la fecha y hora actual
        df_codigo_ciudad['id_estado'] = 1  # Crear el estado del registro como entero
        df_codigo_ciudad['id_estado_registro'] = 1
        df_codigo_ciudad['llaveDuplihis'] = df_codigo_ciudad['sk_distribuidor'].astype(str) + \
            df_codigo_ciudad['cod_distribuidor'].astype(str) + \
            df_codigo_ciudad['fecha_creacion'].astype(str)
        
        merge_df_codigo_ciudad = pd.merge(df_codigo_ciudad, df_his_codigo_ciudad[['llaveDuplihis']], 
                    on=['llaveDuplihis'], 
                    how='left', 
                    indicator=True)
        df_codigo_ciudad_fin = merge_df_codigo_ciudad[merge_df_codigo_ciudad['_merge'] == 'left_only'].drop(columns=['_merge'])

        registros_codigo_ciudad = len(df_codigo_ciudad_fin)
        df_codigo_ciudad_fin.drop(columns=['llaveDuplihis'], inplace=True)

        if registros_codigo_ciudad > 0:
           df_resumen = cargueResumen(
        id_ejecucion_codigo_ciudad, fecha_inicio_tr, fecha_fin_tr, duracion_proceso_seconds,
        'Fuente DWH', registros_codigo_ciudad, 'tb_datos_crudos_dwh_codigo_ciudad', id_estado
        )
        cargueDatos_Codigo_Ciudad(df_codigo_ciudad_fin)
        


        #----------------------------------------------------------------------------------------------------------------------------------------------------
        df_cambioplan = consulta_Cp(engine)# ya esta tb_datos_crudos_dwh_cambio_plan
        df_cambioplan['id_ejecucion'] = id_ejecucion_cambioplan  # Agregar la columna id_ejecucion con el mismo UUID en todas las fila
        df_cambioplan['id'] = [str(uuid.uuid4()) for _ in range(len(df_cambioplan))]  # Agregar la columna id con un UUID único por cada fila
        df_cambioplan['fecha_procesamiento'] = fecha_inicio  # Agregar la columna fecha_procesamiento con la fecha y hora actual
        df_cambioplan['id_estado'] = 1  # Crear el estado del registro como entero
        df_cambioplan['id_estado_registro'] = 1
        df_cambioplan['llaveDuplihis'] = df_cambioplan['co_id'].astype(str) + \
            df_cambioplan['customer_id'].astype(str) + \
            df_cambioplan['tele_numb'].astype(str) + \
            df_cambioplan['cod_plan_origen'].astype(str) + \
            df_cambioplan['cod_plan_destino'].astype(str) + \
            df_cambioplan['fe_creacion'].astype(str) 
        
        merge_df_cambioplan = pd.merge(df_cambioplan, df_his_cambioplan[['llaveDuplihis']], 
                    on=['llaveDuplihis'], 
                    how='left', 
                    indicator=True)
        df_cambioplan_fin = merge_df_cambioplan[merge_df_cambioplan['_merge'] == 'left_only'].drop(columns=['_merge'])
        
        registros_cambioplan = len(df_cambioplan_fin)

        df_cambioplan_fin.drop(columns=['llaveDuplihis'], inplace=True)
        
        if registros_cambioplan > 0:
           df_resumen = cargueResumen(
        id_ejecucion_cambioplan, fecha_inicio_tr, fecha_fin_tr, duracion_proceso_seconds,
        'Fuente DWH', registros_cambioplan, 'tb_datos_crudos_dwh_cambio_plan', id_estado
        )
        cargueDatosBD_Cp(df_cambioplan_fin)
  


        cantidad_registros.append(registros_base_seg+registros_cambioplan+registros_codigo_ciudad+registros_dim_agent+registros_ventas+registros_plan_negocios)



    except Exception as e:
        fuentes.append('Fuentes DWH')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()

# %%


#pd.set_option('display.max_columns', None) 
#pd.set_option('display.max_rows', None)  
#pd.set_option('display.width', None)  
#pd.set_option('display.max_colwidth', None) 

#print(df_cambioplan.dtypes)


