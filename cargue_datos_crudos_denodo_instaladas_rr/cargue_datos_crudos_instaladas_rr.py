"""
***************************************************************************************
* CLARO  HITSS - EMPRESAS Y NEGOCIOS                                                  *
* OBJETIVO: Extración de fuentes crudas de instalada RR                               * 
*           y cargue a base de datos de forma automatica                              *
*           Comunicacion Celular S.A.- Comcel S.A\Wilmer Camargo Ochoa - Data_PCC     *
* TABLA DE INGESTA POSTGRESQL: tb_datos_crudos_denodo_instaladas_rr                   *
* FECHA CREACION: 05 de Septiembre de 2024                                            *
* ELABORADO POR: LAURA GAITAN                                                         *
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
import pyodbc
import sys
sys.path.append('C:/ambiente_desarrollo/dev-empresas-negocios-env/desarrollo_notebook')
import parametros_desarrollo as par
import uuid
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
import psycopg2
import logging
import pyodbc

# %%

#VARIABLES GLOBALES
fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
fecha_actual = datetime.today().date()
duracion = []
fuentes = []
cantidad_registros = []
destino = ['tb_datos_crudos_denodo_instaladas_rr']
estado = []
funcion_error = []
descripcion_error = []
id_ejecucion_en_curso = None

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
        fuentes.append('db_dwh_corporativo.extract_rr_ventas_ins')
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
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,'db_dwh_corporativo.ventas..extract_rr_ventas_ins',0,par.destino_macrofo,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append('db_dwh_corporativo.extract_rr_ventas_ins')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

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
        fuentes.append('db_dwh_corporativo.extract_rr_ventas_ins')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(conexion_BD.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def conexionDenodoOdbc():
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
        usuario_denodo=par.usuario_denodo
        contraseña_denodo=par.contraseña_denodo
        cadena=f'DSN=DenodoODBC;UID={usuario_denodo};PWD={contraseña_denodo}'
        conn = pyodbc.connect(cadena)
        return conn

    except SQLAlchemyError as e:
        fuentes.append('db_dwh_corporativo.extract_rr_ventas_ins')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(conexion_BD.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def ConsultarIformacionAlmacenada(tabla_consulta):
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
    
        
        engine = conexion_BD()
        sql_consulta = f"SELECT * FROM fuentes_cruda.{tabla_consulta}"

        df_tabla_bd = pd.read_sql(sql_consulta, engine)

        return df_tabla_bd
     
       
    except Exception as e:
        fuentes.append('db_dwh_corporativo.extract_rr_ventas_ins')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(ConsultarIformacionAlmacenada.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()
    

# %%
def ejecutarConsultaOdbc():
    """
    Método para ejecutar una consulta y devolver los resultados en un DataFrame.
    """
    conn = conexionDenodoOdbc()
    
    try:
        #cur = conn.cursor()
        query = "SELECT * FROM db_dwh_corporativo.extract_rr_ventas_ins WHERE aumento != 0"
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=[desc[0] for desc in cursor.description])
        print (f'cantidad de registros descargados para instaladas rr: {df.shape[0]}')
        df_resultado = df
        #df_resultado = ordenColumnas(df,1)
        return df_resultado
    
    except pyodbc.Error as e:
        if 1 in estado:
            estado.remove(1)
        if 2 not in estado:
            estado.append(2)
        cantidad_registros.append(0)
        funcion_error.append(ejecutarConsultaOdbc.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
        return None
    finally:
        cursor.close()
        conn.close()

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
        fuentes.append('db_dwh_corporativo.extract_rr_ventas_ins')
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
        fuentes.append('db_dwh_corporativo.extract_rr_ventas_ins')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(generate_uuid.__name__)
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
    log_file = os.path.join(log_directory, "tb_datos_crudos_denodo_instaladas_rr.log")

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
        
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'fuentes_cruda'
        nombre_tabla = 'tb_datos_crudos_denodo_instaladas_rr'
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
       
        
    except SQLAlchemyError as e:
        fuentes.append('db_dwh_corporativo.extract_rr_ventas_ins')
        cantidad_registros.append(0)
        if 1 in estado:
            estado.remove(1)
        if 2 not in estado:
            estado.append(2)
        funcion_error.append(cargueDatosBD.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %%
def seleccionCamposinstaladasrr(df,fecha_inicio_date, id_ejecucion):
    """
    Función encargada de seleccionar los datos relevantes de la base de datos de instaladas RR, 
    extraídos desde Denodo, y generar campos adicionales necesarios para el control 
    y procesamiento de los datos.

    Argumentos:
        df: DataFrame que contiene los datos de instaladas RR a procesar, extraídos de Denodo.
        fecha_inicio_date: Fecha y hora en que comienza el procesamiento de los datos.
        id_ejecucion: Identificador único de la ejecución, utilizado para la creación de claves primarias y foráneas en la base de datos.
    
    Retorna: 
        df_selected: DataFrame final con los datos seleccionados y enriquecidos, listo para ser insertado en la tabla de destino.
    """

    try:
        df_base = df.copy()

        df_base.rename(columns={
            'cvacct':'id_cuenta',
            'cvaon':'ot_orden_trabajo',
            'cvanct':'numero_contrato',
            'cvanom':'nombres',
            'cvatid':'tipo_documento',
            'cvanid':'numero_documento',
            'cvapho':'numero_telefono_1',
            'cvabph':'numero_telefono_2',
            'cvatyp':'tipo_suscriptor_1',
            'cvastr':'numero_calle',
            'cvahom':'direccion_residencia',
            'cvaapt':'numero_apartamento',
            'cvaccd':'ciudad_venta',
            'cvacde':'codigo_division',
            'cvaust':'tipo_suscriptor_2',
            'cvasta':'estado',
            'cvatpe':'codigo_tarifa',
            'cvatys':'nombre_servicio_1',
            'cvarsc':'descripcion_servicio_1',
            'cvaunt':'cvaunt',
            'cvaser':'codigo_servicio',
            'srvnam':'nombre_servicio_2',
            'srvdes':'descripcion_servicio_2',
            'cvasrv':'numero_servicios_1',
            'cvapts':'cvapts',
            'cvadlr':'numero_dealer',
            'cvaase':'nombre_dealer',
            'cvacoo':'coordinador',
            'cvagas':'grupo_dealer',
            'cvasty':'cvasty',
            'cvatsu':'cvatsu',
            'cvacod':'cvacod',
            'cvaarg':'cvaarg',
            'cvatse':'cvatse',
            'cvaknd':'cvaknd',
            'cvavaa':'cvavaa',
            'cvarse':'cvarse',
            'cvanod':'nodo',
            'nodnbr':'nombre_nodo',
            'cvafdi':'fecha_digitacion',
            'cvazon':'cvazon',
            'cvausu':'cvausu',
            'cvafdc':'fecha_creacion_inicio_anio_1',
            'cvafdy':'fecha_creacion_fin_anio_1',
            'cvafdm':'fecha_creacion_mes_1',
            'cvafdd':'fecha_creacion_dia_1',
            'cvafcc':'fecha_creacion_inicio_anio_2',
            'cvafcy':'fecha_creacion_fin_anio_2',
            'cvafcm':'fecha_creacion_mes_2',
            'cvafcd':'fecha_creacion_dia_2',
            'cvafco':'fecha_creacion',
            'cvatvt':'tipo_venta',
            'cvataf':'cvataf',
            'cvapr2':'cvapr2',
            'cvadif':'val_dif_service',
            'cvarme':'valor_servicio',
            'cvarmp':'renta_wo_anterior',
            'cvarmc':'renta_wo_actual',
            'cvarmd':'diferencia_renta',
            'cvalin':'numero_lineas_suscriptor',
            'cvanus':'numero_servicios_2',
            'cvaorg':'origen_datos',
            'cvaca1':'campaña_1',
            'cvaca2':'campaña_2',
            'cvaca3':'campaña_3',
            'cvamig':'cvamig',
            'cvaeto':'estrato',
            'cvinf2':'numeral_2',
            'cvanoc':'conyugue',
            'cvprbf':'cod_black_list',
            'cvprbn':'desc_black_list',
            'cvemfa':'correo1',
            'cvinf1':'correo2',
            'cvfchp':'fecha_permanencia',
            'cvtred':'cvtred',
            'cvstse':'id_servicio',
            'cvdsts':'descripcion_servicio_3',
            'cvrmac':'cvrmac',
            'cvrman':'cvrman',
            'cvndas':'numero_documento_2',
            'cvnase':'detalle_servicio',
            'cvpcam':'cvpcam',
            'cvtpro':'cvtpro',
            'cvtnes':'nobre_especialista',
            'cvtarg':'area_gcia_vtas',
            'cvtzng':'zona_gcia_vtas',
            'cvtcan':'canal',
            'cvtgvd':'aliado',
            'cvtpob':'cvtpob',
            'cvtarv':'area_venta',
            'cvtznv':'zona_venta',
            'tipo_linea':'tipo_red',
            'fecha_sys':'fecha_sys',
            'aumento':'aumento'
        }, inplace=True)

        df_historico = ConsultarIformacionAlmacenada('tb_datos_crudos_denodo_instaladas_rr')

        
        df_base['llave_compuesta'] = (
        df_base['id_cuenta'].astype(str) + '-' +
        df_base['ot_orden_trabajo'].fillna('').astype(str) + '-' +
        df_base['codigo_servicio'].fillna('').astype(str))
        
        df_historico['id_cuenta'] = df_historico ['id_cuenta'].apply(lambda x: int(x))
        df_historico['ot_orden_trabajo'] = df_historico ['ot_orden_trabajo'].apply(lambda x: int(x))
        
        df_historico['llave_compuesta'] = (
        df_historico['id_cuenta'].astype(str) + '-' +
        df_historico['ot_orden_trabajo'].fillna('').astype(str) + '-' +
        df_historico['codigo_servicio'].fillna('').astype(str))
        
    
        df_merged = pd.merge(df_base, df_historico[['llave_compuesta']], on='llave_compuesta', how='outer', indicator=True)
        df_nuevos = df_merged[df_merged['_merge'] == 'left_only'].copy()
        df_nuevos.drop(columns=['_merge'], inplace=True)
        df_nuevos.drop(columns=['llave_compuesta'], inplace=True)
      
        df_nuevos = pd.concat([df_nuevos], ignore_index=True)

        df_nuevos['id'] = [generate_uuid().upper() for _ in range(len(df_nuevos))]
        df_nuevos['id_ejecucion'] = id_ejecucion
        df_nuevos['fecha_procesamiento'] = pd.to_datetime(fecha_inicio_date)
        df_nuevos['fuente'] = 'extract_rr_ventas_ins'
        df_nuevos['id_estado'] = 1

        df_nuevos['correo2'] = df_nuevos['correo2'].str.replace(r'(\S+@\S+\.\S+)\.*', r'\1', regex=True)
        df_nuevos['descripcion_servicio_3'] = df_nuevos['descripcion_servicio_3'].str.replace(r'\.+', '', regex=True)
        df_nuevos['origen_datos'] = df_nuevos['origen_datos'].str.upper()
 
        # Selección de columnas necesarias según la estructura de la tabla final
        
        df_nuevos = df_nuevos[[
            'id', 'id_ejecucion','id_cuenta','ot_orden_trabajo','numero_contrato', 'nombres','tipo_documento','numero_documento',
            'numero_telefono_1','numero_telefono_2','tipo_suscriptor_1','numero_calle','direccion_residencia','numero_apartamento',
            'ciudad_venta', 'codigo_division','tipo_suscriptor_2','estado','codigo_tarifa','nombre_servicio_1','descripcion_servicio_1',
            'cvaunt','codigo_servicio','nombre_servicio_2','descripcion_servicio_2','numero_servicios_1','cvapts','numero_dealer',
            'nombre_dealer','coordinador','grupo_dealer','cvasty','cvatsu','cvacod','cvaarg','cvatse','cvaknd','cvavaa','cvarse',
            'nodo','nombre_nodo','fecha_digitacion','cvazon','cvausu','fecha_creacion_inicio_anio_1','fecha_creacion_fin_anio_1','fecha_creacion_mes_1',
            'fecha_creacion_dia_1','fecha_creacion_inicio_anio_2','fecha_creacion_fin_anio_2','fecha_creacion_mes_2','fecha_creacion_dia_2',
            'fecha_creacion','tipo_venta','cvataf','cvapr2','val_dif_service','valor_servicio','renta_wo_anterior','renta_wo_actual',
            'diferencia_renta','numero_lineas_suscriptor','numero_servicios_2','origen_datos','campaña_1','campaña_2','campaña_3','cvamig','estrato',
            'numeral_2','conyugue','cod_black_list','desc_black_list','correo1','correo2','fecha_permanencia','cvtred','id_servicio','descripcion_servicio_3','cvrmac','cvrman',
            'numero_documento_2','detalle_servicio','cvpcam','cvtpro', 'nobre_especialista','area_gcia_vtas','zona_gcia_vtas', 'canal','aliado','cvtpob','area_venta','zona_venta',
            'tipo_red','fecha_sys','aumento','fecha_procesamiento', 'fuente', 'id_estado'
        ]]
    
    
        return df_nuevos

    except Exception as e:
        fuentes.append('db_dwh_corporativo.extract_rr_ventas_ins')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(seleccionCamposinstaladasrr.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
        raise e  


# %%
if __name__ == "__main__":
    try:
        # Configuración del logging, generación del UUID de ejecución y consulta a la base de datos
        configurarLogging()
        id_ejecucion = generate_uuid().upper()
        fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")

        df_resultado_instaladas_rr = ejecutarConsultaOdbc()
        
        if df_resultado_instaladas_rr is not None:
            registros = len(df_resultado_instaladas_rr)
        
            if registros > 0:
                # Realiza la selección de campos y agrega información adicional
                df_base = seleccionCamposinstaladasrr(df_resultado_instaladas_rr, fecha_inicio_date, id_ejecucion)         
        
                if df_base is not None:
                    
                    conteo_cargue = len(df_base)
                    df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, 'db_dwh_corporativo.extract_rr_ventas_ins', conteo_cargue, 'tb_datos_crudos_denodo_instaladas_rr', 1)
                    cargueDatosBD(df_base)
                    cantidad_registros.append(conteo_cargue)
                    fuentes.append('db_dwh_corporativo.extract_rr_ventas_ins')
                    
                
        fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
        duracion_proceso = fecha_fin_date - fecha_inicio_date
        duracion_proceso_seg = int(duracion_proceso.total_seconds())
        actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date, duracion_proceso_seg)
        duracion.append(str(duracion_proceso))
        estado.append(1)
        salidaLogMonitoreo()
        
        
    except Exception as e:
        fuentes.append('db_dwh_corporativo.extract_rr_ventas_dig')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        




