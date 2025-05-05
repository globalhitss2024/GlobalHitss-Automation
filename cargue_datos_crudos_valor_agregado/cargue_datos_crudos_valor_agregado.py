# %%
"""
***************************************************************************************
* CLARO  HITSS - EMPRESAS Y NEGOCIOS                                                  *
* OBJETIVO: Extración de fuentes crudas de valor agregado                         * 
*           y cargue a base de datos de forma automatica                              *
*           Comunicacion Celular S.A.- Comcel S.A\Wilmer Camargo Ochoa - Data_PCC     *
* TABLA DE INGESTA POSTGRESQL: tb_datos_crudos_valor_agregado                       *
* FECHA CREACION: 27 de Diciembre de 2024                                                 *
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
fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")
duracion = []
fuentes = []
cantidad_registros = []
destino = [par.destino_valor_agregado]
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
        fuentes.append('Valor agregado')
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
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,'valor agregado',0,'tb_datos_crudos_valor_agregado',2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_valor_agregado)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
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
        fuentes.append(par.nombre_archivo_valor_agregado)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(generate_uuid.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    

# %%
def cargueDatosBD(nombre_tabla,df_final):
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

        nombre_esquema = 'fuentes_cruda'
        #print(df_final)
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
    
           
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_valor_agregado)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatosBD.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conexion.dispose()   

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
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(conexion_BD.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()
        print(1)

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
        fuentes.append(par.nombre_archivo_valor_agregado)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(actualizarFechaFinProcesamiento.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def actualizarDatosBD(nombre_tabla, columna ,id, estado_registro, fecha_modificacion):
    """
    Función para actualizar un registro en una tabla de la base de datos.

    Argumentos:
        nombre_tabla (str): El nombre de la tabla en la base de datos.
        columna (str): El nombre de la columna que contiene el ID del registro.
        id (str): El ID del registro a actualizar.
        estado_registro (int): El nuevo estado del registro.
        fecha_modificacion (datetime): La fecha y hora de modificación del registro.

    Retorna:
        None

    Excepciones manejadas:
        SQLAlchemyError as e: Captura errores de SQLAlchemy y los registra en la base de datos de errores.
    """
    
    conexion = conexion_BD()
    if not conexion:
        #print("Error al conectar a la base de datos")
        return

    cursor = conexion.cursor()

    # Preparar la consulta SQL usando placeholders seguros para evitar SQL injection
    query = sql.SQL("""
        UPDATE fuentes_cruda.{table}
        SET id_estado_registro = %s, fecha_modificacion = %s
        WHERE {column} = %s AND id_estado_registro != %s
    """).format(table=sql.Identifier(nombre_tabla), column=sql.Identifier(columna))
    
    parametros = (estado_registro, fecha_modificacion, id, estado_registro)  # Incluye estado_registro dos veces
    try:
        # Ejecutar la consulta
        cursor.execute(query, parametros)
        if cursor.rowcount == 0:
            print("No se requirió actualización o el registro no cumple la condición.")
        else:
            conexion.commit()
            #print("Registro actualizado correctamente.")
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_valor_agregado)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(actualizarDatosBD.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        cursor.close()  
        conexion.close() 


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
    log_file = os.path.join(log_directory, "cargue_datos_crudos_valor_agregado.log")
    
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
def limpiezaCamposString(df):
    for campo in df.select_dtypes(include=['object']).columns:
        df[campo] = df[campo].astype(str) \
                             .str.upper() \
                             .str.strip() \
                             .str.replace('\n', '', regex=True) \
                             .str.replace('\r', '', regex=True) \
                             .str.replace('\t', '', regex=True) \
                             .str.replace('  ', '', regex=True)
    return df
    print(2)

# %%
def consultarTablasPlantaComercialHistorico(tabla_consulta):
    """
    Función que consulta los datos historicos existentes en la base de datos de las tablas de domiminio
    
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
        fuentes.append(par.nombre_archivo_valor_agregado)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarTablasPlantaComercialHistorico.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()
    

# %%
def seleccionCamposValorAgregado(df_combinado, fecha_inicio_date, id_ejecucion):
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
        df_base = df_combinado.copy()

        df_base['id'] = [generate_uuid().upper() for _ in range(len(df_base))]
        df_base['id_ejecucion'] = id_ejecucion
        df_base['fecha_procesamiento'] = fecha_inicio_date
        df_base['fuente'] = par.nombre_archivo_valor_agregado
        df_base['id_estado_registro'] = 1

        df_base = df_base.rename(columns={
            'AÑO DEL REPORTE DE VENTA': 'anio_reporte_venta',
            'MES DEL REPORTE DE VENTA': 'mes_reporte_venta',
            'DIA DEL REPORTE DE VENTA': 'dia_reporte_venta',
            'CODIGO/MARCACION': 'codigo_marcacion',
            'NIT': 'nit',
            'DV- NIT': 'dv_nit',
            'RAZON SOCIAL': 'razon_social',
            'PRODUCTO': 'producto',
            'SERVICIO': 'servicio',
            'PORTAFOLIO': 'portafolio',
            'CEDULA CONSULTOR VENTA': 'cedula_consultor_venta',
            'SEGMENTO': 'segmento',
            'NOMBRE CONSULTOR DE VENTAS': 'nombre_consultor_ventas',
            'CODIGO CONSULTOR DE VENTA': 'codigo_consultor_venta',
            'COORDINADOR': 'coordinador',
            'ESPECIALISTA': 'especialista',
            'JEFE': 'jefe',
            'GERENTE': 'gerente',
            'DIRECTOR': 'director',
            'GERENCIA': 'gerencia',
            'FECHA ACTIVACION': 'fecha_activacion',
            'FECHA DIGITACION': 'fecha_digitacion',
            'BOLSA MINUTOS/SMS INCLUIDOS': 'bolsa_minutos_sms_incluidos',
            'ONE TIME /TVC': 'one_time_tvc',
            'ONE TIME NORMALIZADO': 'one_time_normalizado',
            '# ACTA ENTREGA': 'acta_entrega',
            'No CONTRATO': 'no_contrato',
            'CFM ANT': 'cfm_ant',
            'CFM ACT': 'cfm_act',
            'DIF ENTRE CFM ANT y CFM ACT': 'dif_entre_cfm_ant_y_cfm_act',
            'CFM NETO SIN IVA': 'cfm_neto_sin_iva',
            'PERMANENCIA': 'permanencia',
            'TIPO DE SERVICIO': 'tipo_servicio',
            'LEGALIZADO': 'legalizado',
            'AÑO DEL REPORTE DE LEGALIAZION': 'anio_reporte_legalizacion',
            'MES DEL REPORTE LEGALIZACION': 'mes_reporte_legalizacion',
            'DIA DEL REPORTE LEGALIZACION': 'dia_reporte_legalizacion',
            'OBSERVACIONES': 'observaciones'
        })
        
        # Convertir valores nan a cadenas vacías y fechas a cadenas
        df_base = df_base.applymap(lambda x: '' if pd.isna(x) else str(x) if isinstance(x, pd.Timestamp) else x)
        
        # Convertir columnas específicas de tipo float a 0 si están vacías
        float_columns = ['anio_reporte_venta', 'dia_reporte_venta', 'one_time_tvc', 'one_time_normalizado', 
                         'cfm_ant', 'cfm_act', 'dif_entre_cfm_ant_y_cfm_act', 'cfm_neto_sin_iva', 
                         'anio_reporte_legalizacion', 'dia_reporte_legalizacion']
        for col in float_columns:
            df_base[col] = df_base[col].apply(lambda x: 0 if x == '' else x)
        
        # Dejar solo la fecha en las columnas de fecha
        date_columns = ['fecha_activacion', 'fecha_digitacion']
        for col in date_columns:
            df_base[col] = pd.to_datetime(df_base[col], errors='coerce').dt.strftime('%Y-%m-%d')
       


        # Consultar las tablas de coordinadores, especialistas, jefes y directores
        df_coordinadores = consultarTablasPlantaComercialHistorico('tb_planta_coordinador_directo')
        df_especialistas = consultarTablasPlantaComercialHistorico('tb_planta_especialista')
        df_jefes = consultarTablasPlantaComercialHistorico('tb_planta_jefe')
        df_directores = consultarTablasPlantaComercialHistorico('tb_planta_director')
        df_gerente = consultarTablasPlantaComercialHistorico('tb_planta_gerente')

        # Crear diccionarios para mapear nombres a sus IDs
        coordinador_dict = pd.Series(df_coordinadores.id_coordinador_directo.values, index=df_coordinadores.nombre).to_dict()
        especialista_dict = pd.Series(df_especialistas.id_especialista.values, index=df_especialistas.nombre).to_dict()
        jefe_dict = pd.Series(df_jefes.id_jefe.values, index=df_jefes.nombre).to_dict()
        director_dict = pd.Series(df_directores.id_director.values, index=df_directores.nombre).to_dict()
        gerente_dict = pd.Series(df_gerente.id_gerente.values, index=df_gerente.nombre).to_dict()

        # Limpiar los nombres en df_base
        for col in ['coordinador', 'especialista', 'jefe', 'gerente', 'director']:
            df_base[col] = df_base[col].apply(lambda x: x.replace('(VACANTE)', '').strip())
            df_base[col] = df_base[col].apply(lambda x: x.replace('(VACANTA)', '').strip())


        df_base['id_coordinador'] = df_base['coordinador'].map(coordinador_dict)
        df_base['id_especialista'] = df_base['especialista'].map(especialista_dict)
        df_base['id_jefe'] = df_base['jefe'].map(jefe_dict)
        df_base['id_director'] = df_base['director'].map(director_dict)
        df_base['id_gerente'] = df_base['gerente'].map(gerente_dict)



        df_base = df_base[['id', 'id_ejecucion', 'anio_reporte_venta', 'mes_reporte_venta', 'dia_reporte_venta', 
                   'codigo_marcacion', 'nit', 'dv_nit', 'razon_social', 'producto', 'servicio', 'portafolio', 
                   'cedula_consultor_venta', 'segmento', 'nombre_consultor_ventas', 'codigo_consultor_venta', 
                   'id_coordinador', 'id_especialista', 'id_jefe', 'id_director', 'id_gerente','gerencia', 'fecha_activacion',
                   'fecha_digitacion', 'bolsa_minutos_sms_incluidos', 'one_time_tvc', 'one_time_normalizado', 
                   'acta_entrega', 'no_contrato', 'cfm_ant', 'cfm_act', 'dif_entre_cfm_ant_y_cfm_act', 
                   'cfm_neto_sin_iva', 'permanencia', 'tipo_servicio', 'legalizado', 'anio_reporte_legalizacion', 
                   'mes_reporte_legalizacion', 'dia_reporte_legalizacion', 'observaciones', 'fecha_procesamiento', 
                   'fuente', 'id_estado_registro']]
        
        return df_base
    
    except Exception as e:
        fuentes.append(par.nombre_archivo_valor_agregado)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(seleccionCamposValorAgregado.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def importarvaloragregado(ruta, nombre_archivo, hoja_calculo):
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

        if nombre_archivo == par.nombre_archivo_valor_agregado:
            base_excel = pd.read_excel(ruta + nombre_archivo, sheet_name=hoja_calculo)
        
        return base_excel
        print(f"Archivo importado: {par.nombre_archivo_valor_agregado}, Registros: {len(df_valor_agregado)}")
    except Exception as e:
        logging.error(f"Error al importar archivo {nombre_archivo}: {e}")
        raise


# %%
def cambioDeEstado():
    """
    Este metodo realiza el cambio de id_estado de los cargues anteriores de metas
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

        query = "UPDATE fuentes_cruda.tb_datos_crudos_valor_agregado SET id_estado_registro = 4 WHERE id_estado_registro = 1"
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_metas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cambioDeEstado.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()
    finally:
        conn.close()

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

        df_valor_agregado = importarvaloragregado(par.ruta_fuente_valor_agregado, par.nombre_archivo_valor_agregado, par.nombre_hoja_valor_agregado)
        df_valor_agregado_limpieza = limpiezaCamposString(df_valor_agregado)
        if df_valor_agregado is not None:
                
                registros = len(df_valor_agregado_limpieza)
                
                if registros > 0:
                    df_base = seleccionCamposValorAgregado(df_valor_agregado_limpieza, fecha_inicio_date, id_ejecucion)
                    
                    # Exportar la extracción a un archivo CSV
                    #df_base.to_csv('extraccion_valor_agregado.csv', index=False)
                    
                   
                    registros = len(df_base)
                    cantidad_registros.append(registros)
        
                  
        df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_valor_agregado, registros, par.destino_valor_agregado, 1) 
        cambioDeEstado()  
        cargueDatosBD("tb_datos_crudos_valor_agregado", df_base)
                    
        fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
        duracion_proceso = fecha_fin_date - fecha_inicio_date
        duracion_proceso_seg = int(duracion_proceso.total_seconds())
        actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date, duracion_proceso_seg)   
            
        duracion.append(str(duracion_proceso))
        estado.append(1)
        salidaLogMonitoreo()
    except Exception as e:
        fuentes.append(par.nombre_archivo_valor_agregado)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()


