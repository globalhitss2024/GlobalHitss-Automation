"""
***************************************************************************************
* CLARO  HITSS - EMPRESAS Y NEGOCIOS                                                  *
* OBJETIVO: Extración de fuentes crudas de Legalizadas                                * 
*           y cargue a base de datos de forma automatica                              *
*           Comunicacion Celular S.A.- Comcel S.A\Wilmer Camargo Ochoa - Data_PCC     *
* TABLA DE INGESTA POSTGRESQL: tb_datos_crudos_legalizadas                            *
* FECHA CREACION: 21 de Mayo de 2024                                                  *
* ELABORADO POR: DIEGO SERRANO                                                        *
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
import logging
print('#########################################################################################')
print('#########################################################################################')
print('#########################################################################################')

# %%
#VARIABLES GLOBALES
fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
fecha_actual = datetime.today().date()
duracion = []
fuentes = []
cantidad_registros = []
destino = [par.destino_legalizadas]
estado = []
funcion_error = []
descripcion_error = []
id_ejecucion_en_curso = None
duracion_proceso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
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
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3,0,par.destino_legalizadas,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()
    

# %%
def importarLegalizadasExcel(ruta, nombre_archivo, hoja_calculo):
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
        print(f'Importando archivo {nombre_archivo}')
        return base_excel
    except Exception as e:
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(importarLegalizadasExcel.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

    

# %%
def importarLegalizadasCsv(ruta, nombre_archivo):
    """
    Función que se encarga de importar archivos en formato de csv
    
    Argumentos:
        ruta: variable que contiene la ruta de la fuente
        nombre_archivo: Nombre del archivo
    Retorna: 
        base_csv: Dataframe con los datos provenientes del excel
    Excepciones manejadas: 
        SQLAlchemyError as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    
    try:
        base_csv = pd.read_csv(ruta+nombre_archivo, delimiter=';', encoding='latin1')
        print(f'Importando archivo {nombre_archivo}')
        return base_csv
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(importarLegalizadasCsv.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
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
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
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
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(actualizarFechaFinProcesamiento.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consultarLegalizadasHistorico():
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_legalizadas
    
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
                    from fuentes_cruda.tb_datos_crudos_legalizadas"
        df_legalizadas_historico = pd.read_sql(sql_consulta, engine)
        df_legalizadas_historico = df_legalizadas_historico.drop_duplicates(subset=['ot',])
    
        return df_legalizadas_historico
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarLegalizadasHistorico.__name__)
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
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(generate_uuid.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def seleccionCamposLegalizadas1(base,fecha_inicio_date,id_ejecucion):
    """
    Función que se encarga de seleccionar los datos correspondientes de la base de Legalizadas
    y crea campos adicionales necesarios para el control de los datos
    
    Argumentos:
        base: Dataframe importado previamente y que contiene los datos a procesar
        fecha_inicio_date: Fecha de inicio del procesamiento
        id_ejecucion: Contiene un numero alfanumerico para creación de llaves primarias y foraneas de la base de datos
    Retorna: 
        df_base: Dataframe con los datos seleccionados y agregados
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    
    try:
    
        df_base = base.copy()
        df_base = df_base [['Cuenta','OT','Fecha Ultimo Estado']]
        df_base['Fecha Ultimo Estado'] = pd.to_datetime(df_base['Fecha Ultimo Estado'], origin='1899-12-30', unit='D')
        df_base = df_base.rename(columns={'Cuenta' : 'cuenta'})
        df_base = df_base.rename(columns={'OT' : 'ot'})
        df_base = df_base.rename(columns={'Fecha Ultimo Estado' : 'fecha_legalizada'})
        df_base ['fecha_procesamiento']=  fecha_inicio_date
        df_base ['id_estado'] = '1'
        df_base['fuente'] = 'Legalizadas'
        df_base['id'] = df_base.apply(lambda row: generate_uuid().upper(), axis=1)
        df_base['id_ejecucion'] = id_ejecucion
        df_base['ot'] = df_base ['ot'].apply(lambda x: str(x))
        df_base['cuenta'] = df_base ['cuenta'].apply(lambda x: str(x))
        df_base = df_base.drop_duplicates(subset=['ot',])
        df_base= df_base[(df_base['ot'].notnull()) & (df_base['cuenta'].notnull())]
        df_base = df_base[['id','id_ejecucion','cuenta','ot','fecha_legalizada','fecha_procesamiento','fuente','id_estado']]
    
    except Exception as e:
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(seleccionCamposLegalizadas1.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    
    return df_base
    
    

# %%
def seleccionCamposLegalizadas2(base,fecha_inicio_date,id_ejecucion):
    """
    Función que se encarga de seleccionar los datos correspondientes de la base de Legalizadas Correo
    y crea campos adicionales necesarios para el control de los datos
    
    Argumentos:
        base: Dataframe importado previamente y que contiene los datos a procesar
        fecha_inicio_date: Fecha de inicio del procesamiento
        id_ejecucion: Contiene un numero alfanumerico para creación de llaves primarias y foraneas de la base de datos
    Retorna: 
        df_base: Dataframe con los datos seleccionados y agregados
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    
    try:
    
        df_base = base.copy()
        df_base = df_base [['ID','OT','FECHA DE LEGALIZACION']]
        df_base = df_base.rename(columns={'ID' : 'cuenta'})
        df_base = df_base.rename(columns={'OT' : 'ot'})
        df_base = df_base.rename(columns={'FECHA DE LEGALIZACION' : 'fecha_legalizada'})
        df_base['fecha_legalizada'] = pd.to_datetime(df_base['fecha_legalizada']).dt.strftime('%Y-%m-%d')
        df_base ['fecha_procesamiento']=  fecha_inicio_date
        df_base ['id_estado'] = '1'
        df_base['fuente'] = 'Legalizadas Correos'
        df_base['id'] = df_base.apply(lambda row: generate_uuid().upper(), axis=1)
        df_base['id_ejecucion'] = id_ejecucion
        df_base['ot'] = df_base ['ot'].apply(lambda x: str(x))
        df_base['cuenta'] = df_base ['cuenta'].apply(lambda x: str(x))
        df_base = df_base.drop_duplicates(subset=['ot',])
        df_base= df_base[(df_base['ot'].notnull()) & (df_base['cuenta'].notnull())]
        df_base = df_base[['id','id_ejecucion','cuenta','ot','fecha_legalizada','fecha_procesamiento','fuente','id_estado']]
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(seleccionCamposLegalizadas2.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    
    return df_base

# %%
def seleccionCamposLegalizadas3(base,fecha_inicio_date,id_ejecucion):
    """
    Función que se encarga de seleccionar los datos correspondientes de la base de Legalizadas Ubicacion Contrato
    y crea campos adicionales necesarios para el control de los datos
    
    Argumentos:
        base: Dataframe importado previamente y que contiene los datos a procesar
        fecha_inicio_date: Fecha de inicio del procesamiento
        id_ejecucion: Contiene un numero alfanumerico para creación de llaves primarias y foraneas de la base de datos
    Retorna: 
        df_base: Dataframe con los datos seleccionados y agregados
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    
    try:
    
        df_base = base.copy()
        df_base = df_base [['Cuenta','O.T.','Fecha último estado']]
        df_base = df_base.rename(columns={'Cuenta' : 'cuenta'})
        df_base = df_base.rename(columns={'O.T.' : 'ot'})
        df_base = df_base.rename(columns={'Fecha último estado' : 'fecha_legalizada'})
        df_base['fecha_legalizada'] = df_base['fecha_legalizada'].str.split().str[0] 
        df_base['fecha_legalizada'] = pd.to_datetime(df_base['fecha_legalizada'], format='%d/%m/%Y', dayfirst=True).dt.strftime('%Y-%m-%d')
        df_base ['fecha_procesamiento']=  fecha_inicio_date
        df_base ['id_estado'] = '1'
        df_base['fuente'] = 'Legalizadas Ubicacion Contrato'
        df_base['id'] = df_base.apply(lambda row: generate_uuid().upper(), axis=1)
        df_base['id_ejecucion'] = id_ejecucion
        df_base['ot'] = df_base ['ot'].apply(lambda x: str(x))
        df_base['cuenta'] = df_base ['cuenta'].apply(lambda x: str(x))
        df_base = df_base.drop_duplicates(subset=['ot',])
        df_base= df_base[(df_base['ot'].notnull()) & (df_base['cuenta'].notnull())]
        df_base = df_base[['id','id_ejecucion','cuenta','ot','fecha_legalizada','fecha_procesamiento','fuente','id_estado']]
        
        return df_base
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(seleccionCamposLegalizadas3.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
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
        nombre_tabla = 'tb_datos_crudos_legalizadas'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
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
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
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
    log_file = os.path.join(log_directory, "cargue_datos_crudos_legalizadas.log")

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
        id_ejecucion = generate_uuid().upper() #Id de ejecución temporal por si falla antes de entrar al primer if
        id_ejecucion_en_curso = id_ejecucion
        fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")
        archivos = [f for f in os.listdir(par.ruta_fuente_legalizadas) if archivo_modificado_hoy(os.path.join(par.ruta_fuente_legalizadas, f))]
        archivos_actualizados = archivos
        df_legalizadas_historico = consultarLegalizadasHistorico()

        if par.nombre_archivo_legalizadas1 in archivos_actualizados:  
            #fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            #fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")
            id_ejecucion = generate_uuid().upper()
            id_ejecucion_en_curso = id_ejecucion
            base_excel_legalizadas1 = importarLegalizadasExcel(par.ruta_fuente_legalizadas, par.nombre_archivo_legalizadas1, par.nombre_hoja_legalizadas1)
            df_seleccion_campos_legalizadas1 = seleccionCamposLegalizadas1(base_excel_legalizadas1,fecha_inicio_date,id_ejecucion)
            #inicio de identificacion de registros nuevos
            #colliers_fuera_dominio_subzona =  pd.merge(colliers_no_nulos, tabla_subzona , left_on=['Sub_zona','ID_Ciudad'], right_on= ['nombre_subzona','ID_Ciudad'],how='outer', indicator=True).query('_merge == "left_only"')
            df_legalizadas1_nuevas = pd.merge(df_seleccion_campos_legalizadas1, df_legalizadas_historico, left_on='ot', right_on= ['ot'], how='outer', indicator=True, suffixes=('_legalizadas', '_historico')).query('_merge == "left_only"')
            columnas_seleccionadas = [par.mapeo_columnas[col] for col in par.mapeo_columnas.keys() if par.mapeo_columnas[col] in df_legalizadas1_nuevas.columns]
            df_legalizadas1_nuevas = df_legalizadas1_nuevas[columnas_seleccionadas]
            df_legalizadas1_nuevas.columns = df_legalizadas1_nuevas.columns.str.replace('_legalizadas', '')
            #fin de identificacion de registros nuevos
            fuentes.append(par.nombre_archivo_legalizadas1)
            registros = len(df_legalizadas1_nuevas)
            cantidad_registros.append(registros)
            
            if registros > 0:
                df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_legalizadas1,registros,par.destino_legalizadas,1)
                cargueDatosBD(df_legalizadas1_nuevas)

            else:
                df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_legalizadas1,registros,par.destino_legalizadas,1)
                cargueDatosBD(df_legalizadas1_nuevas)

            fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
            duracion_proceso = fecha_fin_date - fecha_inicio_date
            duracion_proceso_seg = int(duracion_proceso.total_seconds())
            actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date,duracion_proceso_seg)

        if par.nombre_archivo_legalizadas2 in archivos_actualizados:
            #fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            #fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")
            id_ejecucion = generate_uuid().upper()
            id_ejecucion_en_curso = id_ejecucion 
            base_excel_legalizadas2 = importarLegalizadasExcel(par.ruta_fuente_legalizadas, par.nombre_archivo_legalizadas2, par.nombre_hoja_legalizadas2)
            df_seleccion_campos_legalizadas2 = seleccionCamposLegalizadas2(base_excel_legalizadas2,fecha_inicio_date,id_ejecucion)
            #inicio de identificacion de registros nuevos
            df_legalizadas2_nuevas = pd.merge(df_seleccion_campos_legalizadas2, df_legalizadas_historico, left_on='ot', right_on= ['ot'], how='outer', indicator=True, suffixes=('_legalizadas', '_historico')).query('_merge == "left_only"')
            columnas_seleccionadas = [par.mapeo_columnas[col] for col in par.mapeo_columnas.keys() if par.mapeo_columnas[col] in df_legalizadas2_nuevas.columns]
            df_legalizadas2_nuevas = df_legalizadas2_nuevas[columnas_seleccionadas]
            df_legalizadas2_nuevas.columns = df_legalizadas2_nuevas.columns.str.replace('_legalizadas', '')
            #fin de identificacion de registros nuevos
            fuentes.append(par.nombre_archivo_legalizadas2)
            registros = len(df_legalizadas2_nuevas)
            cantidad_registros.append(registros)
            
            if registros > 0:
                df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date,par.nombre_archivo_legalizadas2,registros,par.destino_legalizadas,1)
                cargueDatosBD(df_legalizadas2_nuevas)
            
            else:
                df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_legalizadas2,registros,par.destino_legalizadas,1)
                cargueDatosBD(df_legalizadas1_nuevas)
                
            fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
            duracion_proceso = fecha_fin_date - fecha_inicio_date
            duracion_proceso_seg = int(duracion_proceso.total_seconds())
            actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date,duracion_proceso_seg)


        if par.nombre_archivo_legalizadas3 in archivos_actualizados:
            #fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            #fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")
            id_ejecucion = generate_uuid().upper()
            id_ejecucion_en_curso = id_ejecucion
            base_excel_legalizadas3 = importarLegalizadasCsv(par.ruta_fuente_legalizadas, par.nombre_archivo_legalizadas3)
            df_seleccion_campos_legalizadas3 = seleccionCamposLegalizadas3(base_excel_legalizadas3,fecha_inicio_date,id_ejecucion)
            #inicio de identificacion de registros nuevos
            df_legalizadas3_nuevas = pd.merge(df_seleccion_campos_legalizadas3, df_legalizadas_historico, left_on='ot', right_on= ['ot'], how='outer', indicator=True, suffixes=('_legalizadas', '_historico')).query('_merge == "left_only"')
            columnas_seleccionadas = [par.mapeo_columnas[col] for col in par.mapeo_columnas.keys() if par.mapeo_columnas[col] in df_legalizadas3_nuevas.columns]
            df_legalizadas3_nuevas = df_legalizadas3_nuevas[columnas_seleccionadas]
            df_legalizadas3_nuevas.columns = df_legalizadas3_nuevas.columns.str.replace('_legalizadas', '')
            #fin de identificacion de registros nuevos
            fuentes.append(par.nombre_archivo_legalizadas3)
            registros = len(df_legalizadas3_nuevas)
            cantidad_registros.append(registros)
            
            if registros > 0:
                df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date,par.nombre_archivo_legalizadas3,registros,par.destino_legalizadas,1) 
                cargueDatosBD(df_legalizadas3_nuevas)

            else:
                df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_legalizadas3,registros,par.destino_legalizadas,1)
                cargueDatosBD(df_legalizadas1_nuevas)
                
            fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
            duracion_proceso = fecha_fin_date - fecha_inicio_date
            duracion_proceso_seg = int(duracion_proceso.total_seconds())
            actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date,duracion_proceso_seg)   

        duracion.append(str(duracion_proceso))
        estado.append(1)
        salidaLogMonitoreo()
        print(salidaLogMonitoreo())

    except Exception as e:
        fuentes.append(par.nombre_archivo_legalizadas1+" | "+par.nombre_archivo_legalizadas2+" | "+par.nombre_archivo_legalizadas3)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()



