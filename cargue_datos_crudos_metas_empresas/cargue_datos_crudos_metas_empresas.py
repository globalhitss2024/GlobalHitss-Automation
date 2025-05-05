# %%
"""
***************************************************************************************
* CLARO  HITSS - EMPRESAS Y NEGOCIOS                                                  *
* OBJETIVO: Extración de fuentes crudas de Metas (Carmen Bejarano)                    * 
*           y cargue a base de datos de forma automatica                              *
*           Comunicacion Celular S.A.- Comcel S.A\Wilmer Camargo Ochoa - Data_PCC     *
* TABLA DE INGESTA POSTGRESQL: tb_datos_crudos_metas_oficial                          *
* FECHA CREACION: 30 de Diciembre de 2024                                             *
* ELABORADO POR: Danilo Rodríguez                                                     *
* *************************************************************************************
* MODIFICACIONES
* NOMBRE                   FECHA      VERSION            DESCRIPCION
* 
*
***************************************************************************************
"""

# %%
import pandas as pd
import urllib3
urllib3.disable_warnings()
from datetime import datetime
import sys
sys.path.append('C:/ambiente_desarrollo/dev-empresas-negocios-env/desarrollo_produccion')
import parametros_produccion as par
import uuid
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
import psycopg2
from psycopg2 import sql
import logging
import openpyxl
from openpyxl import load_workbook
pd.set_option('display.max_columns', None) 

# %%
#VARIABLES GLOBALES
fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
fecha_actual = datetime.today().date()
duracion = []
fuentes = []
cantidad_registros = []
destino = [par.destino_metas_empresas]
estado = []
funcion_error = []
descripcion_error = []
id_ejecucion_en_curso = None

# %%
def salidaLogMonitoreo():
    """
    Este metodo captura la informacion que se desea imprimir en el Log
    para monitoreo y funcionamiento del desarrollo
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        None
    """
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
    Este método captura la información que se desea imprimir en el Log
    para monitoreo y funcionamiento del desarrollo.
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
 """

# %%
def conexion_BD():
    """
    Función que genera la conexión hacia la base de datos por medio de la libreria psycopg2
    
    Argumentos:
        None
    Retorna: 
        conn: Conexion con la base de datos
    Excepciones manejadas: 
        SQLAlchemyError as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        conn = psycopg2.connect(
            host= par.host,
            port= par.port,
            dbname= par.bd_inteligencia_comercial,
            user= par.usuario,
            password= par.contrasena
        )
        return conn
    
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_metas_empresas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(conexion_BD.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

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
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,par.nombre_archivo_metas_empresas,0,par.destino_metas_empresas,2) 
        salidaLogMonitoreo()

    except SQLAlchemyError as e:
        fuentes.append(par.ruta_fuente_metas_empresas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date, duracion_proceso_seg):
    """
    Función que actualiza la fecha fin de procesamiento y duración para el proceso que se ejecuto.
    Utilizando cursores
    
    Argumentos:
        id_ejecucion: id del proceso ejecutado
        fecha_fin_date: Fecha fin de procesamiento
        duracion_proceso_seg: Duración en segundos del procesamiento
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        
        conn = conexion_BD()
        cur = conn.cursor()

        update_query = """
            UPDATE control_procesamiento.tb_resumen_cargue 
            SET fecha_fin_procesamiento = %s,
            duracion_segundos = %s
            WHERE id_ejecucion = %s
        """
        cur.execute(update_query, (fecha_fin_date, duracion_proceso_seg, id_ejecucion))
        conn.commit()
        cur.close()
        conn.close()
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_metas_empresas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(actualizarFechaFinProcesamiento.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def generate_uuid():
    """
    Función que genera un numero alfanumerico para creación de llaves primarias y foraneas
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    
    try:
        return str(uuid.uuid4())
    
    except Exception as e:
        fuentes.append(par.nombre_archivo_metas_empresas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(generate_uuid.__name__)
        insertarErroresDB()
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def archivo_modificado_hoy(ruta_archivo):
    """
    Función que indica la fecha de actualización de las fuentes
    
    Argumentos:
        ruta_archivo: Contiene la ruta donde se encuentran los archivos fuente
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    
    try:
        fecha_modificacion = datetime.fromtimestamp(os.path.getmtime(ruta_archivo)).date()
        return fecha_modificacion == fecha_actual
    
    except Exception as e:
        fuentes.append(par.nombre_archivo_metas_empresas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(archivo_modificado_hoy.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def importarMetasEmpresas(ruta, hoja_calculo_metas_empresas):
    """
    Este metodo realiza la importacion de las metas oficial de la fuente cruda
    Argumentos:
        ruta: ruta donde se encuentra el archivo
        hoja_calculo_metas_oficial: nombre de la hoja de calculo del canal fijo
    Retorna: 
        base_excel_metas_oficial: dataframe con los datos de la fuente cruda
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        # Lista los archivos de la ruta
        files = os.listdir(ruta)

        # Unir la ruta con el nombre del archivo para obtener la ruta completa
        full_paths = [os.path.join(ruta, file) for file in files]

        # Obtener el archivo más reciente de la ruta
        newest_file = max(full_paths, key=os.path.getctime)
    
        base_excel_metas_empresas = pd.read_excel(newest_file, sheet_name=hoja_calculo_metas_empresas)
        return base_excel_metas_empresas
    
    except Exception as e:
        fuentes.append(par.nombre_archivo_metas_empresas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(importarMetasEmpresas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def seleccionCamposMetasEmpresas(df_excel, fecha_inicio_date, id_ejecucion):
    """
    Función que se encarga de añadir campos necesarios o faltantes para el cargue a la base de datos

    Argumentos:
        df_combinado: Contiene el dataframe que se requiere para añadir los campos
        fecha_inicio_date: Fecha de inicio de procesamiento
        id_ejecucion: ID de ejecucion
    Retorna: 
        df_base: Retorna el dataframe con los campos faltantes
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:    
        df_excel.columns = df_excel.columns.str.strip()
        df_excel.columns = df_excel.columns.str.upper()

        df_base = df_excel[['TIPO', 'CÉDULA', 'NOMBRE', 'CARGO', 'NOMBRE GERENTE', 'DIRECTOR',
                   'ALTAS MÓVIL', 'BAJAS MÓVIL', 'CAMBIO DE PLAN', 'RETO ESTRATÉGICO MÓVIL', 'NETO MÓVIL',
                   'INCENTIVO UNIDADES MÓVIL', 'LÍNEAS BAJAS', 'LÍNEAS NETAS']].copy()
    
        df_base['id'] = [uuid.uuid4() for _ in range(len(df_base))]
        df_base['id_ejecucion'] = id_ejecucion
        df_base['fecha_procesamiento'] = fecha_inicio_date
        df_base['fuente'] = par.nombre_archivo_metas_empresas
        df_base['id_estado_registro'] = 1
    
        df_base = df_base.rename(columns ={
            'TIPO' : 'tipo',
            'CÉDULA': 'identificacion', 
            'NOMBRE': 'nombre',
            'CARGO': 'cargo',
            'NOMBRE GERENTE': 'nombre_gerente',
            'DIRECTOR': 'director',
            'ALTAS MÓVIL': 'altas_movil',
            'BAJAS MÓVIL': 'bajas_movil',
            'CAMBIO DE PLAN': 'cambio_plan',
            'RETO ESTRATÉGICO MÓVIL': 'reto_estrategico_movil',
            'NETO MÓVIL': 'neto_movil',
            'INCENTIVO UNIDADES MÓVIL': 'incentivo_unidades_movil',
            'LÍNEAS BAJAS': 'lineas_bajas',
            'LÍNEAS NETAS': 'lineas_netas'

        })

        df_base = df_base[['id','id_ejecucion', 'tipo', 'identificacion', 'nombre', 'cargo', 'nombre_gerente', 'director', 'altas_movil', 'bajas_movil', 'cambio_plan', 'reto_estrategico_movil', 'neto_movil','incentivo_unidades_movil','lineas_bajas','lineas_netas', 'fecha_procesamiento', 'fuente', 'id_estado_registro']]
        return df_base
    
    except Exception as e:
        fuentes.append(par.nombre_archivo_metas_oficial)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(seleccionCamposMetasEmpresas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
def cargueDatosBD(df_final):
    """
    Este metodo realiza el cargue de los datos a la base de datos
    Argumentos:
        df_final: dataframe con los datos de la fuente cruda
    Retorna: 
        None
    Excepciones manejadas:
        SQLAlchemyError as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial}')
    
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'fuentes_cruda'
        nombre_tabla = 'tb_datos_crudos_metas_empresas'

        df_final.to_sql(nombre_tabla, con = conexion, schema=nombre_esquema, if_exists='append', index=False)
    
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_metas_empresas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatosBD.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %%
def cargueResumen(id_ejecucion, fecha_inicio_date,fuentes,cantidad_registros,destino,estado):
    """
    Función que se encarga de cargar estadisticas de los datos que estan siendo procesados
    
    Argumentos:
        id_ejecucion: Contiene un numero alfanumerico para creación de llaves primarias y foraneas de la base de datos
        fecha_inicio_date: Fecha de inicio del procesamiento
        fecha_fin_date: Fecha de fin del procesamiento
        duracion_proceso: Duración del procesamiento 
        fuentes: Fuentes de donde provienen los datos
        cantidad_registros: Cantidad de registros procesados
        destino: Tabla donde se ingestan los datos
        estado: Indica el estado del proceso de acuerdo a lo definido en la base de datos en la tabla control_procesamiento.estados_cargue 
    Retorna: 
        None
    Excepciones manejadas: 
        SQLAlchemyError as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        
        df_resumen_cargue = pd.DataFrame({
            'id_ejecucion': id_ejecucion,
            'fecha_inicio_procesamiento': fecha_inicio_date,
            'fuentes': fuentes,
            'cantidad_registros': cantidad_registros,
            'destino': [destino],
            'id_estado': [estado],
        })

        #errores de conexion se ponen a mano
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_resumen_cargue'
        
        df_resumen_cargue.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)

    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_metas_empresas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueResumen.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %%
def cambioDeEstado():
    """
    Este metodo realiza el cambio de id_estado de los cargues anteriores de metas oficial
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        conn = conexion_BD()
        cursor = conn.cursor()

        query = "UPDATE fuentes_cruda.tb_datos_crudos_metas_empresas SET id_estado_registro = 4 WHERE id_estado_registro = 1"
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_metas_empresas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cambioDeEstado.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()
    finally:
        conn.close()

# %%
def limpiezaCamposString(df):
    for campo in df.select_dtypes(include=['object']).columns:
        df[campo] = df[campo].apply(lambda x: x.upper().strip() \
                                    .replace('\n', '') \
                                    .replace('\r', '') \
                                    .replace('\t', '') \
                                    .replace('  ', '') \
                                    if isinstance(x, str) else x)
    return df

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
    log_file = os.path.join(log_directory, "cargue_datos_crudos_metas_empresas.log")

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
if __name__ == '__main__':
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
            id_ejecucion = generate_uuid().upper()
            id_ejecucion_en_curso = id_ejecucion
            
            fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")

            archivos = [f for f in os.listdir(par.ruta_fuente_metas_empresas) if archivo_modificado_hoy(os.path.join(par.ruta_fuente_metas_empresas))]
            archivo_actualizados = archivos

            if par.nombre_archivo_metas_empresas in archivo_actualizados:
                 
                #Importar metas oficial
                df_excel = importarMetasEmpresas(par.ruta_fuente_metas_empresas, par.hoja_calculo_metas_empresas)

                #Limpieza de campos
                df_limpiado = limpiezaCamposString(df_excel)

                #Asignacion de campos faltantes
                df_base = seleccionCamposMetasEmpresas(df_limpiado, fecha_actual, id_ejecucion_en_curso)

                registros = len(df_base)
                cantidad_registros.append(registros)

                if registros > 0:
                    df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_metas_empresas, registros, par.destino_metas_empresas, 1)
                    cambioDeEstado()
                    cargueDatosBD(df_base)

                fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
                duracion_proceso = fecha_fin_date - fecha_inicio_date
                duracion_proceso_seg = int(duracion_proceso.total_seconds())
                actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date, duracion_proceso_seg)

            duracion.append(str(duracion_proceso))
            estado.append(1)
            salidaLogMonitoreo()

        except Exception as e:
            fuentes.append(par.nombre_archivo_metas_empresas)
            cantidad_registros.append(0)
            estado.append(2)
            funcion_error.append("__main__")
            descripcion_error.append(str(e)[:100])
            salidaLogMonitoreo()


