"""
***************************************************************************************
* CLARO  HITSS - EMPRESAS Y NEGOCIOS                                                  *
* OBJETIVO: Data Cruda base maestro códigos                                           *                                                                            *
* TABLA DE INGESTA POSTGRESQL: tbl_crudo_maestro_codigos                              *
* FECHA CREACION: 7 de junio de 2024                                                  *
* ELABORADO POR: DANILO RODRIGUEZ                                                     *
* *************************************************************************************
* MODIFICACIONES
* NOMBRE                   FECHA      VERSION            DESCRIPCION
* 
*
***************************************************************************************
"""

# %%
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine 
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
import urllib3
urllib3.disable_warnings()
import sys
sys.path.append('C:/ambiente_desarrollo/dev-empresas-negocios-env/desarrollo_produccion')
import parametros_produccion as par
import logging
import uuid
import os

# %%
#VARIABLES GLOBALES
fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
fecha_actual = datetime.today().date()
duracion = []
fuentes = []
cantidad_registros = []
destino = [par.destino_maestro_codigos]
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

        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_resumen_cargue'
        
        df_resumen_cargue.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)


    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_maestro_codigos)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueResumen.__name__)
        descripcion_error.append(str(e)[:100])
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
        
        conexion_errores = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_errores_cargue'
        errores.to_sql(nombre_tabla, con=conexion_errores, schema=nombre_esquema, if_exists='append', index=False)
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,par.nombre_archivo_maestro_codigos,0,par.destino_maestro_codigos,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_maestro_codigos)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def importarMaestroCodigosExcel(ruta, nombre_archivo, hoja_calculo):
    """
    Función que se encarga de importar archivos en formato de excel
    
    Argumentos:
        ruta: variable que contiene la ruta de la fuente
        nombre_archivo: Nombre del archivo
        hoja_calculo: Pestaña del archivo de excel donde se encuentran los datos
    Retorna: 
        base_excel: Dataframe con los datos provenientes del excel
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    
    try:
    
        base_excel = pd.read_excel(ruta+nombre_archivo, sheet_name=hoja_calculo)
    except Exception as e:
        fuentes.append(par.nombre_archivo_maestro_codigos)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(importarMaestroCodigosExcel.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

    return base_excel

# %%
def conexion_BD():
    """
    Función que genera la conexión hacia la base de datos por medio de la libreria psycopg2
    
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
        
        conn = psycopg2.connect(
            host=par.host,
            database=par.bd_inteligencia_comercial,
            user=par.usuario,
            password=par.contrasena
        )
        return conn

    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_maestro_codigos)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(conexion_BD.__name__)
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
        fuentes.append(par.nombre_archivo_maestro_codigos)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(actualizarFechaFinProcesamiento.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consultarMaestroCodigosHistorico():
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_maestro_codigos
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        
        engine = conexion_BD()
        sql_consulta = "Select * \
                    from fuentes_cruda.tb_datos_crudos_maestro_codigos"
        df_maestro_codigos_historico = pd.read_sql(sql_consulta, engine)
        df_maestro_codigos_historico = df_maestro_codigos_historico.drop_duplicates(subset=['saserv',])
        
    
        return df_maestro_codigos_historico
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_maestro_codigos)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarMaestroCodigosHistorico.__name__)
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
        fuentes.append(par.nombre_archivo_maestro_codigos)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(generate_uuid.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def limpiezaCamposMaestroCodigos(base, fecha_inicio_date, id_ejecucion):
    """
    Función que se encarga de limpiar los datos del dataframe instaladas. Y crea campos adicionales necesarios para el control de los datos
    
    Argumentos:
        base: Dataframe importado previamente y que contiene los datos a procesar.
        fecha_inicio_date: Fecha de inicio del procesamiento.
        id_ejecucion: Contiene un número alfanumérico para creación de llaves primarias y foráneas de la base de datos.
    
    Retorna: 
        df_base: Dataframe con los datos seleccionados y agregados.
    
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    
    try:
        df_base = base.copy()
        df_base = df_base [[
           'SASERV', 'SANAME', 'SACATG', 'SAJURI', 'SASGRP', 'CLASIFICACION', 'CLAS_PLANEACION', 'Servicios', 'Velocidad Kbps', 'P&G Gestion',
           'Resumen Comentarios', 'Producto Homologado', 'Cebe Sinergia', 'CeBe Fusión', 'Obs:', 'Tecnologia Principal', 'Plataforma',
           'TV con HD', 'CAPACIDAD_NAVEGACION'
        ]]
        df_base.rename(columns = {
            'SASERV': 'saserv', 
            'SANAME': 'saname', 
            'SACATG': 'sacatg',
            'SAJURI': 'sajuri',
            'SASGRP': 'sasgrp',
            'CLASIFICACION': 'clasificacion',
            'CLAS_PLANEACION': 'clas_planeacion',
            'Servicios': 'servicios',
            'Velocidad Kbps': 'velocidad_kbps',
            'P&G Gestion': 'p_g_gestion',
            'Resumen Comentarios': 'resumen_comentarios',
            'Producto Homologado': 'producto_homologado',
            'Cebe Sinergia': 'cebe_sinergia',
            'CeBe Fusión': 'cebe_fusion',
            'Obs:': 'observaciones',
            'Tecnologia Principal': 'tecnologia_principal',
            'Plataforma': 'plataforma',
            'TV con HD': 'tv_hd',
            'CAPACIDAD_NAVEGACION': 'capacidad_navegacion'
        }, inplace=True)
        df_base = df_base.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df_base = df_base.applymap(lambda x: x.upper() if isinstance(x, str) else x)
        df_base.fillna({
            'sacatg': 'NR',
            'sajuri': 'NR',
            'sasgrp': 'NR',
            'clasificacion': 'NO REGISTRA',
            'clas_planeacion': 'NO REGISTRA',
            'servicios': 'NO REGISTRA',
            'velocidad_kbps': 0,
            'p_g_gestion': 'NO REGISTRA',
            'resumen_comentarios': 'NO REGISTRA',
            'producto_homologado': 'NO REGISTRA',
            'cebe_sinergia': 'NO REGISTRA',
            'cebe_fusion': 'NO REGISTRA',
            'observaciones': 'NO REGISTRA',
            'tecnologia_principal': 'NO REGISTRA',
            'plataforma': 'NO REGISTRA',
            'tv_hd': 'NO REGISTRA'
        }, inplace=True)
        df_base['clasificacion'].replace('TELEVISIÓN', 'TELEVISION', inplace=True)
        df_base['capacidad_navegacion'] = pd.to_numeric(df_base['capacidad_navegacion'], errors='coerce').fillna(0)
        df_base['cargar_servicio'] = np.where(df_base['clas_planeacion'] == 'SOPORTE PC', False, True)
        df_base['id'] = df_base.apply(lambda row: generate_uuid().upper(), axis=1)
        df_base['id_ejecucion'] = id_ejecucion
        df_base['fecha_procesamiento'] = fecha_inicio_date
        df_base['fuente'] = 'MAESTRO DE CODIGOS CORREOS'
        df_base ['id_estado'] = '1'
        orden_columnas = ['id', 'id_ejecucion'] + [col for col in df_base.columns if col not in ['id', 'id_ejecucion']]
        df_base = df_base.reindex(columns=orden_columnas)
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_maestro_codigos)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(limpiezaCamposMaestroCodigos.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    
    return df_base


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
        nombre_tabla = 'tb_datos_crudos_maestro_codigos'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_maestro_codigos)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatosBD.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conexion.dispose()



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
        fuentes.append(par.nombre_archivo_maestro_codigos)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(archivo_modificado_hoy.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


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
    log_file = os.path.join(log_directory, "cargue_datos_crudos_maestro_codigos.log")

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
        id_ejecucion = generate_uuid().upper()
        id_ejecucion_en_curso = id_ejecucion
        fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")
        archivos = [f for f in os.listdir(par.ruta_fuente_maestro_codigos) if archivo_modificado_hoy(os.path.join(par.ruta_fuente_maestro_codigos, f))]
        archivos_actualizados = archivos
        df_maestro_codigos_historico = consultarMaestroCodigosHistorico()

        # Verifica si el archivo del día existe
        if par.nombre_archivo_maestro_codigos not in archivos_actualizados:
            # Si no se encuentra el archivo del día, agrega un valor cero a la base
            id_ejecucion = generate_uuid().upper()
            id_ejecucion_en_curso = id_ejecucion
            df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_maestro_codigos, 0, par.destino_maestro_codigos, 1)
            cantidad_registros.append(0)
        else:
            # Si el archivo del día está presente, sigue con la lógica original
            id_ejecucion = generate_uuid().upper()
            id_ejecucion_en_curso = id_ejecucion
            base_excel_maestro_codigos = importarMaestroCodigosExcel(par.ruta_fuente_maestro_codigos, par.nombre_archivo_maestro_codigos, par.nombre_hoja_maestro_codigos)
            df_limpieza_campos_maestro_codigos = limpiezaCamposMaestroCodigos(base_excel_maestro_codigos, fecha_inicio_date, id_ejecucion)
            df_maestro_codigos_nuevo = pd.merge(df_limpieza_campos_maestro_codigos, df_maestro_codigos_historico, left_on='saserv', right_on=['saserv'], how='outer', indicator=True, suffixes=('_maestro_codigos', '_historico')).query('_merge == "left_only"')
            columnas_seleccionadas = [par.mapeo_columnas_maestro_codigos[col] for col in par.mapeo_columnas_maestro_codigos.keys() if par.mapeo_columnas_maestro_codigos[col] in df_maestro_codigos_nuevo.columns]
            df_maestro_codigos_nuevo = df_maestro_codigos_nuevo[columnas_seleccionadas]
            df_maestro_codigos_nuevo.columns = df_maestro_codigos_nuevo.columns.str.replace('_maestro_codigos', '')
            fuentes.append(par.nombre_archivo_maestro_codigos)
            registros = len(df_maestro_codigos_nuevo)
            cantidad_registros.append(registros)
            
            if registros > 0:
                df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_maestro_codigos, registros, par.destino_maestro_codigos, 1)
                cargueDatosBD(df_maestro_codigos_nuevo)
            else:
                df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_maestro_codigos, registros, par.destino_maestro_codigos, 1)
                cargueDatosBD(df_maestro_codigos_nuevo)

        fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
        duracion_proceso = fecha_fin_date - fecha_inicio_date
        duracion_proceso_seg = int(duracion_proceso.total_seconds())
        actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date, duracion_proceso_seg)

        duracion.append(str(duracion_proceso))
        estado.append(1)
        salidaLogMonitoreo()

    except Exception as e:
        fuentes.append(par.nombre_archivo_maestro_codigos)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()



