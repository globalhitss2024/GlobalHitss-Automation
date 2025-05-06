# %%
"""
***************************************************************************************
* CLARO  HITSS - EMPRESAS Y NEGOCIOS                                                  *
* OBJETIVO: Data Cruda base canceladas 999                                            *                                                                            *
* TABLA DE INGESTA POSTGRESQL: tbl_dados_crudos_canceladas_999                        *
* FECHA CREACION: 18 de junio de 2024                                                 *
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
from datetime import datetime
from sqlalchemy import create_engine 
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
import urllib3
urllib3.disable_warnings()
import sys
sys.path.append('C:/ambiente_desarrollo/dev-empresas-negocios-env/desarrollo_produccion')
import parametros_produccion as par
from datetime import datetime, timedelta
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
destino = [par.destino_canceladas_999]
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
        fuentes.append(par.nombre_archivo_canceladas_999)
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
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,par.nombre_archivo_canceladas_999,0,par.destino_canceladas_999,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_canceladas_999)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def importarCanceladas999Excel(ruta, nombre_archivo, hoja_calculo):
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
        fuentes.append(par.nombre_archivo_canceladas_999)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(importarCanceladas999Excel.__name__)
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
        fuentes.append(par.nombre_archivo_canceladas_999)
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
        fuentes.append(par.nombre_archivo_canceladas_999)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(actualizarFechaFinProcesamiento.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consultarCanceladas999Historico():
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_instaladas_999
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        
        engine = conexion_BD()
        sql_consulta = """
        SELECT *
        FROM fuentes_cruda.tb_datos_crudos_canceladas_999
        WHERE valor_servicio > 0
        """
        df_instaladas_999_historico = pd.read_sql(sql_consulta, engine)
        #df_instaladas_999_historico = df_instaladas_999_historico.drop_duplicates(subset=['ot',])
    
        return df_instaladas_999_historico
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_canceladas_999)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarCanceladas999Historico.__name__)
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
        fuentes.append(par.nombre_archivo_canceladas_999)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(generate_uuid.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def limpiezaCamposCanceladas999(base, fecha_inicio_date, id_ejecucion):
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
        df_base.rename(columns={
            'CUENTA': 'cuenta',
            'ORDEN_TRABAJO': 'ot',
            'NOMBRES': 'nombres',
            'NUMERO_TELEFONO_1': 'numero_telefono_1',
            'NUMERO_TELEFONO_2': 'numero_telefono_2',
            'CALLE': 'calle',
            'DIRECCION': 'direccion',
            'NUMERO_APARTAMENTO': 'numero_apartamento',
            'CIUDAD_VENTA': 'ciudad_venta',
            'CODIGO_DIVISION': 'codigo_division',
            'TIPO_SUSCRIPTOR': 'tipo_suscriptor',
            'ESTADO': 'estado',
            'CODIGO_SERVICIO': 'codigo_servicio',
            'NOMBRE_SERVICIO': 'nombre_servicio',
            'DESCRIPCION_SERVICIO': 'descripcion_servicio',
            'VALOR_SERVICIO': 'valor_servicio',
            'NUMERO_ASESOR': 'numero_asesor',
            'NOMBRE_ASESOR': 'nombre_asesor',
            'TIPO_ORDEN': 'tipo_orden',
            'USUARIO_CREADOR': 'usuario_creador',
            'FECHA_CREACION': 'fecha_creacion',
            'HORA_CREACION': 'hora_creacion',
            'FECHA_CANCELADA': 'fecha_cancelada',
            'HORA_CANCELADA': 'hora_cancelada',
            'MIGRACION': 'migracion',
            'CODIGO_CANCELACION': 'codigo_cancelacion',
            'DESCRIPCION_CANCELACION': 'descripcion_cancelacion',
            'ESTRATO': 'estrato',
            'NUMERAL_2': 'numeral_2',
            'CONYUGUE': 'conyugue',
            'COD_BLACK_LIST': 'cod_black_list',
            'DESC_BLACK_LIST': 'desc_black_list',
            'EMAIL': 'email',
            'INF_ADICI_01': 'inf_adici_01',
            'FECHA_PERMANENCIA': 'fecha_permanencia'
        }, inplace=True)
        df_base.fillna({
            'nombres': 'NO REGISTRA',
            'numero_telefono_1': 'NO REGISTRA',
            'numero_telefono_2': 'NO REGISTRA',
            'calle': 'NO REGISTRA',
            'direccion': 'NO REGISTRA',
            'numero_apartamento': 'NO REGISTRA',
            'ciudad_venta': 'NO REGISTRA',
            'codigo_division': 'NO REGISTRA',
            'tipo_suscriptor': 'NO REGISTRA',
            'estado': 'NO REGISTRA',
            'nombre_servicio': 'NO REGISTRA',
            'descripcion_servicio': 'NO REGISTRA',
            'valor_servicio': 0,
            'numero_asesor': '0',
            'nombre_asesor': 'NO REGISTRA',
            'tipo_orden': 'NO REGISTRA',
            'usuario_creador': 'NO REGISTRA',
            'migracion': 'NO REGISTRA',
            'codigo_cancelacion': 'NR',
            'descripcion_cancelacion': 'NO REGISTRA',
            'estrato': 'NO REGISTRA',
            'numeral_2': 'NO REGISTRA',
            'conyugue': 'NO REGISTRA',
            'cod_black_list': 'NO REGISTRA',
            'desc_black_list': 'NO REGISTRA',
            'email': 'NO REGISTRA',
            'inf_adici_01': 'NO REGISTRA'
        }, inplace=True)

        df_historico = consultarCanceladas999Historico()
        df_base = df_base[df_base['valor_servicio'] > 0]
        df_base['llave_compuesta'] = (
        df_base['cuenta'].astype(str) + '-' +
        df_base['ot'].fillna('').astype(str) + '-' +
        df_base['codigo_servicio'].fillna('').astype(str))
        
        df_historico['ot'] = df_historico ['ot'].apply(lambda x: int(x))
        df_historico['cuenta'] = df_historico ['cuenta'].apply(lambda x: int(x))
        
        df_historico['llave_compuesta'] = (
        df_historico['cuenta'].astype(str) + '-' +
        df_historico['ot'].fillna('').astype(str) + '-' +
        df_historico['codigo_servicio'].fillna('').astype(str))


        df_merged = pd.merge(df_base, df_historico[['llave_compuesta']], on='llave_compuesta', how='outer', indicator=True)
        df_nuevos = df_merged[df_merged['_merge'] == 'left_only'].copy()
        df_nuevos.drop(columns=['_merge'], inplace=True)
        df_nuevos.drop(columns=['llave_compuesta'], inplace=True)

        df_nuevos = pd.concat([df_nuevos], ignore_index=True)

        df_nuevos['fecha_creacion'].fillna('1900-01-01', inplace=True)
        df_nuevos['hora_creacion'] = df_nuevos['hora_creacion'].fillna(datetime(1899, 12, 30))
        df_nuevos['fecha_cancelada'].fillna('1900-01-01', inplace=True)
        df_nuevos['hora_cancelada'] = df_nuevos['hora_cancelada'].fillna(datetime(1899, 12, 30))
        df_nuevos['fecha_permanencia'].fillna('1900-01-01', inplace=True)
        df_nuevos['fecha_permanencia'] = pd.to_datetime(df_nuevos['fecha_permanencia'], errors='coerce').fillna(datetime(1900, 1, 1))
        df_nuevos = df_nuevos.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df_nuevos = df_nuevos.applymap(lambda x: x.upper() if isinstance(x, str) else x)
        df_nuevos['id'] = df_nuevos.apply(lambda row: generate_uuid().upper(), axis=1)
        df_nuevos['id_ejecucion'] = id_ejecucion
        df_nuevos['fecha_procesamiento'] = fecha_inicio_date
        df_nuevos['fuente'] = 'CANCELADAS 999 CORREOS'
        df_nuevos['id_estado'] = '1'
        orden_columnas = ['id', 'id_ejecucion'] + [col for col in df_nuevos.columns if col not in ['id', 'id_ejecucion']]
        df_nuevos = df_nuevos.reindex(columns=orden_columnas)
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_canceladas_999)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(limpiezaCamposCanceladas999.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    
    return df_nuevos


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
        nombre_tabla = 'tb_datos_crudos_canceladas_999'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_canceladas_999)
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
        fuentes.append(par.nombre_archivo_canceladas_999)
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
    log_file = os.path.join(log_directory, "cargue_datos_crudos_canceladas_999.log")

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
        archivos = [f for f in os.listdir(par.ruta_fuente_canceladas_999) if archivo_modificado_hoy(os.path.join(par.ruta_fuente_canceladas_999, f))]
        archivos_actualizados = archivos
        #df_canceladas_999_historico = consultarCanceladas999Historico()
        print(f'Archivo encontrado: {archivos_actualizados}')

        if par.nombre_archivo_canceladas_999 in archivos_actualizados:  
            id_ejecucion = generate_uuid().upper()
            id_ejecucion_en_curso = id_ejecucion
            base_excel_canceladas_999 = importarCanceladas999Excel(par.ruta_fuente_canceladas_999, par.nombre_archivo_canceladas_999, par.nombre_hoja_canceladas_999)
            df_canceladas_999_nuevo = limpiezaCamposCanceladas999(base_excel_canceladas_999,fecha_inicio_date,id_ejecucion)
            #df_canceladas_999_nuevo = pd.merge(df_limpieza_campos_canceladas_999, df_canceladas_999_historico, left_on='ot', right_on= ['ot'], how='outer', indicator=True, suffixes=('_canceladas_999', '_historico')).query('_merge == "left_only"')
            #columnas_seleccionadas = [par.mapeo_columnas_canceladas_999[col] for col in par.mapeo_columnas_canceladas_999.keys() if par.mapeo_columnas_canceladas_999[col] in df_canceladas_999_nuevo.columns]
            #df_canceladas_999_nuevo = df_canceladas_999_nuevo[columnas_seleccionadas]
            #df_canceladas_999_nuevo.columns = df_canceladas_999_nuevo.columns.str.replace('_canceladas_999', '')
            fuentes.append(par.nombre_archivo_canceladas_999)
            registros = len(df_canceladas_999_nuevo)
            cantidad_registros.append(registros)
                
            if registros > 0:
                df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_canceladas_999,registros,par.destino_canceladas_999,1)
                cargueDatosBD(df_canceladas_999_nuevo)
            
            else:
                df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_canceladas_999,registros,par.destino_canceladas_999,1)
                cargueDatosBD(df_canceladas_999_nuevo)
            
            fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
            duracion_proceso = fecha_fin_date - fecha_inicio_date
            duracion_proceso_seg = int(duracion_proceso.total_seconds())
            actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date,duracion_proceso_seg)

            duracion.append(str(duracion_proceso))
            estado.append(1)
            salidaLogMonitoreo()
        else:
            print('\nArchivo base Canceladas 999 no encontrado')
    except Exception as e:
        fuentes.append(par.nombre_archivo_canceladas_999)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()



