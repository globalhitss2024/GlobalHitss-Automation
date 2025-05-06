# %%
"""
***************************************************************************************
* CLARO  HITSS - EMPRESAS Y NEGOCIOS                                                  *
* OBJETIVO: Data Cruda base transfers                                                *                                                                            *
* TABLA DE INGESTA POSTGRESQL: tbl_crudo_instaladas, tbl_crudo_instaladas_999,        *
    tbl_crudo_instaladas_up, tbl_crudo_digitadas, tbl_crudo_canceladas,               *
    tbl_crudo_canceladas_999, tbl_crudo_instaladas_up                                 *
* FECHA CREACION: 19 de junio de 2024                                                  *
* ELABORADO POR: MARIO ALBERTO PUELLO MONTERROSA                                      *
* *************************************************************************************
* MODIFICACIONES
* NOMBRE                   FECHA      VERSION            DESCRIPCION
* 
*
***************************************************************************************
"""

# %%
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine 
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
import urllib3
import re
import gc
urllib3.disable_warnings()
import sys
sys.path.append('C:/ambiente_desarrollo/dev-empresas-negocios-env/desarrollo_produccion')
import parametros_produccion as par
import logging
import uuid
import os
import time

# %%
#VARIABLES GLOBALES
fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
fecha_actual = datetime.today().date()
duracion = []
fuentes = []
cantidad_registros = []
destino = [par.nombre_destino_transfers]
estado = []
funcion_error = []
descripcion_error = []
id_ejecucion_en_curso = None
global transfer
transfer=pd.DataFrame(columns=par.mapeo_columnas_transfers)
fecha_control=pd.DataFrame()
fecha_control_swp=pd.DataFrame()

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
        destino: Tabla donde se insertan los datos
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
        fuentes.append(par.nombre_fuentes_transfers)
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
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,par.nombre_archivo_instaladas,0,par.destino_instaladas,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_fuentes_transfers)
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
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial}')
        return conexion

    except SQLAlchemyError as e:
        fuentes.append(par.nombre_fuentes_transfers)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(conexion_BD.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def conexion_BD_2():
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
        #print('realizando conexion con psycopg2')
        conn = psycopg2.connect(
            host=par.host,
            database=par.bd_inteligencia_comercial,
            user=par.usuario,
            password=par.contrasena
        )
        return conn

    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_instaladas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(conexion_BD.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()


# %%
def actualizarFechaFinProcesamiento(id_ejecucion_proceso, fecha_fin_date, duracion_proceso_seg):

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
        conn = conexion_BD_2()
        cur = conn.cursor()

        update_query = """
            UPDATE control_procesamiento.tb_resumen_cargue 
            SET fecha_fin_procesamiento = %s,
            duracion_segundos = %s
            WHERE id_ejecucion = %s
        """
        cur.execute(update_query, (fecha_fin_date, duracion_proceso_seg, id_ejecucion_proceso))
        conn.commit()
        cur.close()
        conn.close()
        
    except Exception as e:
        fuentes.append(par.nombre_fuentes_transfers)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(actualizarFechaFinProcesamiento.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consultarTransfersHistoricos():
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_transfers
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        engine = conexion_BD()
        sql_consulta = "SELECT fecha_procesamiento FROM fuentes_cruda.tb_datos_crudos_transfers ORDER BY fecha_procesamiento DESC LIMIT 10 "
        #print(f'consulta historica: {sql_consulta}')
        df_transfers_historico = pd.read_sql(sql_consulta, engine)
  
        return df_transfers_historico
        
    except Exception as e:
        fuentes.append(par.destino_transfers)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarTransfersHistoricos.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consultarCrudoInstaladas(fecha_historico):
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_instaladas
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        engine = conexion_BD()
        sql_consulta = f"Select * \
                    from fuentes_cruda.tb_datos_crudos_instaladas where fecha_procesamiento >= '{fecha_historico}'"
        df_instaladas_crudo = pd.read_sql(sql_consulta, engine)
        df_instaladas_crudo=df_instaladas_crudo.rename(columns=par.mapeo_columnas_instaladas_principal)
        
        pd.set_option('display.max_columns', None)
        df_instaladas_crudo['cuenta'] = df_instaladas_crudo['cuenta'].astype(str)
        df_instaladas_crudo['orden_trabajo'] = df_instaladas_crudo['orden_trabajo'].astype(str)
        df_instaladas_crudo['val_dif_service'] = df_instaladas_crudo['val_dif_service'].astype(int).fillna(0)
        df_instaladas_crudo['valor_servicio'] = df_instaladas_crudo['valor_servicio'].astype(int).fillna(0)
        df_instaladas_crudo['renta_wo_anterior'] = df_instaladas_crudo['renta_wo_anterior'].astype(int).fillna(0)
        df_instaladas_crudo['renta_wo_actual'] = df_instaladas_crudo['renta_wo_actual'].astype(int).fillna(0)
        df_instaladas_crudo['diferencia_renta'] = df_instaladas_crudo['diferencia_renta'].astype(int).fillna(0)
        df_instaladas_crudo.loc[df_instaladas_crudo['origen_datos']=='A','origen_datos']='CROSS SELLING'
        df_instaladas_crudo.loc[df_instaladas_crudo['origen_datos']=='NEW','valor_neto']=df_instaladas_crudo['valor_servicio']
        df_instaladas_crudo.loc[df_instaladas_crudo['origen_datos']=='CROSS SELLING','valor_neto']=df_instaladas_crudo['valor_servicio']
        df_instaladas_crudo.loc[df_instaladas_crudo['origen_datos']=='UP SELLING','valor_neto']=df_instaladas_crudo['val_dif_service']                    
        df_instaladas_crudo['estado_2']='INSTALADA'
        df_instaladas_crudo['tipo_v']='ALTAS_F'
        df_instaladas_crudo['tipo'] = 'ALTAS_F'
        df_instaladas_crudo['base'] = 'FIJO'
        df_instaladas_crudo['adicional_inf1']=''
        df_instaladas_crudo['fuente']=par.nombre_fuente_instaladas
        print(f'cantidad de registros Instaladas: {df_instaladas_crudo.shape[0]}')

                 
        return df_instaladas_crudo
        
    except Exception as e:
        fuentes.append(par.nombre_fuente_instaladas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarCrudoInstaladas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consultarCrudoInstaladas999(fecha_historico):
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_instaladas
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        engine = conexion_BD()
        sql_consulta = f"Select * from fuentes_cruda.tb_datos_crudos_instaladas_999 where fecha_procesamiento >= '{fecha_historico}'"
        df_instaladas_crudo = pd.read_sql(sql_consulta, engine)
        df_instaladas_crudo=df_instaladas_crudo.rename(columns=par.mapeo_columnas_instaladas_principal)
        pd.set_option('display.max_columns', None)
        df_instaladas_crudo['cuenta'] = df_instaladas_crudo['cuenta'].astype(str)
        df_instaladas_crudo['orden_trabajo'] = df_instaladas_crudo['orden_trabajo'].astype(str)
        df_instaladas_crudo['val_dif_service'] = df_instaladas_crudo['val_dif_service'].astype(int).fillna(0)
        df_instaladas_crudo['valor_servicio'] = df_instaladas_crudo['valor_servicio'].astype(int).fillna(0)
        df_instaladas_crudo['renta_wo_anterior'] = df_instaladas_crudo['renta_wo_anterior'].astype(int).fillna(0)
        df_instaladas_crudo['renta_wo_actual'] = df_instaladas_crudo['renta_wo_actual'].astype(int).fillna(0)
        df_instaladas_crudo['diferencia_renta'] = df_instaladas_crudo['diferencia_renta'].astype(int).fillna(0)
        df_instaladas_crudo.loc[df_instaladas_crudo['origen_datos']=='A','origen_datos']='CROSS SELLING'
        df_instaladas_crudo.loc[df_instaladas_crudo['origen_datos']=='NEW','valor_neto']=df_instaladas_crudo['valor_servicio']
        df_instaladas_crudo.loc[df_instaladas_crudo['origen_datos']=='CROSS SELLING','valor_neto']=df_instaladas_crudo['valor_servicio']
        df_instaladas_crudo.loc[df_instaladas_crudo['origen_datos']=='UP SELLING','valor_neto']=df_instaladas_crudo['val_dif_service']                    
        df_instaladas_crudo['estado_2']='INSTALADA'
        df_instaladas_crudo['tipo_v']='ALTAS_F'
        df_instaladas_crudo['tipo'] = 'ALTAS_F'
        df_instaladas_crudo['base'] = 'FIJO'
        df_instaladas_crudo['adicional_inf1']=''
        df_instaladas_crudo['fuente']=par.nombre_fuente_instaladas999
        print(f'cantidad de registros Instaladas 999: {df_instaladas_crudo.shape[0]}')
                 
        return df_instaladas_crudo
        
    except Exception as e:
        fuentes.append(par.nombre_fuente_instaladas999)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarCrudoInstaladas999.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consultarCrudoInstaladasUp(fecha_historico):
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_instaladas
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        engine = conexion_BD()
        sql_consulta = f"Select * from fuentes_cruda.tb_datos_crudos_digitadas_up where fecha_procesamiento >= '{fecha_historico}'"
        #print (sql_consulta)
        df_instaladasup_crudo = pd.read_sql(sql_consulta, engine)
        df_instaladasup_crudo=df_instaladasup_crudo.rename(columns=par.mapeo_columnas_instaladas_up_transfer)
        df_instaladasup_crudo['cuenta'] = df_instaladasup_crudo['cuenta'].astype(str)
        df_instaladasup_crudo['orden_trabajo'] = df_instaladasup_crudo['orden_trabajo'].astype(str)
        df_instaladasup_crudo['monthly_rental_charge'] = df_instaladasup_crudo['monthly_rental_charge'].astype(int)
        df_instaladasup_crudo['renta_mes_anterior'] = df_instaladasup_crudo['renta_mes_anterior'].astype(int)
        df_instaladasup_crudo['estado_2']='INSTALADA'
        df_instaladasup_crudo['tipo_v']='ALTAS_F'
        df_instaladasup_crudo['tipo'] = 'ALTAS_F'
        df_instaladasup_crudo['base'] = 'FIJO'
        df_instaladasup_crudo['valor_neto']=df_instaladasup_crudo['monthly_rental_charge'] - df_instaladasup_crudo['renta_mes_anterior']
        df_instaladasup_crudo['renta_wo_actual']=df_instaladasup_crudo['monthly_rental_charge'] 
        df_instaladasup_crudo['renta_wo_anterior']=df_instaladasup_crudo['monthly_rental_charge'] 
        df_instaladasup_crudo['fuente']=par.nombre_fuente_instaladasup
        print(f'cantidad de registros Instaladas up: {df_instaladasup_crudo.shape[0]}')


        return df_instaladasup_crudo
        
    except Exception as e:
        fuentes.append(par.nombre_fuente_instaladasup)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarCrudoInstaladasUp.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consultarCrudoDigitadas(fecha_historico):
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_instaladas
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        engine = conexion_BD()
        sql_consulta = f"Select * from fuentes_cruda.tb_datos_crudos_digitadas where fecha_procesamiento >= '{fecha_historico}'"
        df_digitadas_crudo = pd.read_sql(sql_consulta, engine)
        df_digitadas_crudo=df_digitadas_crudo.rename(columns=par.mapeo_columnas_digitadas_principal)
        df_digitadas_crudo['cuenta'] = df_digitadas_crudo['cuenta'].astype(str)
        df_digitadas_crudo['orden_trabajo'] = df_digitadas_crudo['orden_trabajo'].astype(str)
        df_digitadas_crudo['val_dif_service'] = df_digitadas_crudo['val_dif_service'].astype(int).fillna(0)
        df_digitadas_crudo['valor_servicio'] = df_digitadas_crudo['valor_servicio'].astype(int).fillna(0)
        df_digitadas_crudo['renta_wo_anterior'] = df_digitadas_crudo['renta_wo_anterior'].astype(int).fillna(0)
        df_digitadas_crudo['renta_wo_actual'] = df_digitadas_crudo['renta_wo_actual'].astype(int).fillna(0)
        df_digitadas_crudo['diferencia_renta'] = df_digitadas_crudo['diferencia_renta'].astype(int).fillna(0)
        df_digitadas_crudo.loc[df_digitadas_crudo['origen_datos']=='A','origen_datos']='CROSS SELLING'
        df_digitadas_crudo.loc[df_digitadas_crudo['origen_datos']=='NEW','valor_neto']=df_digitadas_crudo['valor_servicio']
        df_digitadas_crudo.loc[df_digitadas_crudo['origen_datos']=='CROSS SELLING','valor_neto']=df_digitadas_crudo['valor_servicio']
        df_digitadas_crudo.loc[df_digitadas_crudo['origen_datos']=='UP SELLING','valor_neto']=df_digitadas_crudo['val_dif_service']    
        df_digitadas_crudo['estado_2']='DIGITADA'
        df_digitadas_crudo['tipo_v']='VENTAS_F'
        df_digitadas_crudo['tipo'] = 'DIGITADO'
        df_digitadas_crudo['base'] = 'FIJO'
        df_digitadas_crudo['fuente']=par.nombre_fuente_digitadas
        print(f'cantidad de registros Digitadas: {df_digitadas_crudo.shape[0]}')

        return df_digitadas_crudo

    except Exception as e:
        fuentes.append(par.nombre_fuente_digitadas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarCrudoDigitadas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consultarCrudoDigitadasUp(fecha_historico):
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_instaladas
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        engine = conexion_BD()
        sql_consulta = f"Select * from fuentes_cruda.tb_datos_crudos_digitadas_up where fecha_procesamiento >= '{fecha_historico}'"
        df_digitadasup_crudo = pd.read_sql(sql_consulta, engine)
        df_digitadasup_crudo=df_digitadasup_crudo.rename(columns=par.mapeo_columnas_digitadas_up_transfer)
        df_digitadasup_crudo['cuenta'] = df_digitadasup_crudo['cuenta'].astype(str)
        df_digitadasup_crudo['orden_trabajo'] = df_digitadasup_crudo['orden_trabajo'].astype(str)
        df_digitadasup_crudo['monthly_rental_charge'] = df_digitadasup_crudo['monthly_rental_charge'].astype(int)
        df_digitadasup_crudo['renta_mes_anterior'] = df_digitadasup_crudo['renta_mes_anterior'].astype(int)
        df_digitadasup_crudo['estado_2']='DIGITADA'
        df_digitadasup_crudo['tipo_v']='VENTAS_F'
        df_digitadasup_crudo['tipo'] = 'DIGITADO'
        df_digitadasup_crudo['base'] = 'FIJO'
        df_digitadasup_crudo['valor_neto']=df_digitadasup_crudo['monthly_rental_charge'] - df_digitadasup_crudo['renta_mes_anterior']
        df_digitadasup_crudo['renta_wo_actual']=df_digitadasup_crudo['monthly_rental_charge'] 
        df_digitadasup_crudo['renta_wo_anterior']=df_digitadasup_crudo['monthly_rental_charge'] 
        df_digitadasup_crudo['fuente']=par.nombre_fuente_digitadasup
        print(f'cantidad de registros Digitadas up: {df_digitadasup_crudo.shape[0]}')


        return df_digitadasup_crudo
        
    except Exception as e:
        fuentes.append(par.nombre_fuente_digitadasup)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarCrudoDigitadasUp.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consultarCrudoCanceladas(fecha_historico):
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_instaladas
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        engine = conexion_BD()
        sql_consulta = f"Select * from fuentes_cruda.tb_datos_crudos_canceladas where fecha_procesamiento >= '{fecha_historico}'"
        df_canceladas_crudo = pd.read_sql(sql_consulta, engine)
        df_canceladas_crudo=df_canceladas_crudo.rename(columns=par.mapeo_columnas_canceladas_principal)
        df_canceladas_crudo['cuenta'] = df_canceladas_crudo['cuenta'].astype(str)
        df_canceladas_crudo['orden_trabajo'] = df_canceladas_crudo['orden_trabajo'].astype(str)
        df_canceladas_crudo['val_dif_service'] = 0
        df_canceladas_crudo['valor_servicio'] = df_canceladas_crudo['valor_servicio'].astype(int).fillna(0)
        df_canceladas_crudo['renta_wo_anterior'] = 0
        df_canceladas_crudo['renta_wo_actual'] = 0
        df_canceladas_crudo['diferencia_renta'] = 0
        df_canceladas_crudo['estado_2']='CANCELADA'
        df_canceladas_crudo['tipo_v']='CANCELADAS_F'
        df_canceladas_crudo['tipo'] = 'CANCELADAS_F'
        df_canceladas_crudo['base'] = 'FIJO'
        df_canceladas_crudo['valor_neto']=0
        df_canceladas_crudo['fuente']=par.nombre_fuente_canceladas
        print(f'cantidad de registros Canceladas: {df_canceladas_crudo.shape[0]}')

        return df_canceladas_crudo

    except Exception as e:
        fuentes.append(par.nombre_fuente_canceladas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarCrudoCanceladas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consultarCrudoCanceladas999(fecha_historico):
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_instaladas
    
    Argumentos:
        None
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        engine = conexion_BD()
        sql_consulta = f"Select * from fuentes_cruda.tb_datos_crudos_canceladas where fecha_procesamiento >= '{fecha_historico}'"
        df_canceladas_crudo = pd.read_sql(sql_consulta, engine)
        df_canceladas999_crudo=df_canceladas_crudo.rename(columns=par.mapeo_columnas_canceladas999)
        df_canceladas999_crudo['cuenta'] = df_canceladas999_crudo['cuenta'].astype(str)
        df_canceladas999_crudo['orden_trabajo'] = df_canceladas999_crudo['orden_trabajo'].astype(str)
        df_canceladas999_crudo['val_dif_service'] = 0
        df_canceladas999_crudo['valor_servicio'] = df_canceladas999_crudo['valor_servicio'].astype(int).fillna(0)
        df_canceladas999_crudo['renta_wo_anterior'] = 0
        df_canceladas999_crudo['renta_wo_actual'] = 0
        df_canceladas999_crudo['diferencia_renta'] = 0
        df_canceladas999_crudo['estado_2']='CANCELADA'
        df_canceladas999_crudo['tipo_v']='CANCELADAS_F'
        df_canceladas999_crudo['tipo'] = 'CANCELADAS_F'
        df_canceladas999_crudo['base'] = 'FIJO'
        df_canceladas999_crudo['valor_neto']=0
        df_canceladas999_crudo['fuente']=par.nombre_fuente_canceladas999
        print(f'cantidad de registros Canceladas 999: {df_canceladas999_crudo.shape[0]}')
        return df_canceladas999_crudo

    except Exception as e:
        fuentes.append(par.nombre_fuente_canceladas999)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarCrudoCanceladas999.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def eliminarCaracteres(val):
    try:
        # Si el valor es una cadena, eliminar los caracteres especificados
        if isinstance(val, str):
            return re.sub(r'\.+', '', val)  # Reemplaza una o más secuencias de puntos con una cadena vacía

        # Si el valor no es una cadena (por ejemplo, es numérico), devolverlo sin cambios
        else:
            return val
    except Exception as e:
        fuentes.append('def eliminarCaracteres()')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(eliminarCaracteres.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
def adicionRegistrosTransfers(base):
    """
    Función que se encarga de concatenar dataframe de entrada con el dataframe de transfers.
    
    Argumentos:
        base: Dataframe importado previamente y que contiene los datos a procesar.
    
    Retorna: 
        df_base: Dataframe con los datos seleccionados y agregados.
    
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    #global transfer
    try:
        df_base = base.copy()
        #df_base = df_base.applymap(lambda x: x.strip() if isinstance(x, str) else x) #si es un str, elimine espacios al principio y al final, sino deje el mismo dato
        #df_base = df_base.applymap(lambda x: x.upper() if isinstance(x, str) else x) #si es un str, convierta a mayuscula, sino deje el mismo dato
        df_base = df_base.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
        df_base = df_base.apply(lambda col: col.map(lambda x: x.upper() if isinstance(x, str) else x))

        columnas=par.mapeo_columnas_transfers
        df_transformada=pd.DataFrame(columns=columnas)

        for col in df_base.columns:
            if col in df_transformada.columns:
                df_transformada[col]=df_base[col]

        df_transformada['llave_proceso'] = df_transformada['orden_trabajo'] + df_transformada['codigo_servicio'] + df_transformada['tipo_v']
        #transfer_swp=pd.concat([transfer,df_transformada ], ignore_index=True) 
        #transfer=transfer_swp

        return df_transformada
       
    except Exception as e:
        fuentes.append(par.nombre_fuentes_transfers)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(adicionRegistrosTransfers.__name__)
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
        fuentes.append(par.nombre_fuentes_transfers)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(generate_uuid.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def CamposControlTransfers(base, fecha_inicio_date, id_ejecucion_proceso):
    """
    Función que se encarga de limpiar los datos del dataframe suministrado. Y crea campos adicionales necesarios para el control de los datos
    
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
        #df = base.copy()
        df_base = base.drop_duplicates(subset=['llave_proceso']).copy()
        df_base['id'] = df_base.apply(lambda row: generate_uuid().upper(), axis=1)
        df_base['id_ejecucion'] = id_ejecucion_proceso
        df_base['fecha_procesamiento'] = fecha_inicio_date
        df_base['id_estado'] = '1'
        orden_columnas = ['id', 'id_ejecucion'] + [col for col in df_base.columns if col not in ['id', 'id_ejecucion']]
        df_base = df_base.reindex(columns=orden_columnas)
        return df_base
        
    except Exception as e:
        fuentes.append(par.nombre_fuentes_transfers)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(CamposControlTransfers.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    
    


# %%
def limpiezaCamposDataframe(base,nombre_archivo ):
    """
    Función que se encarga de limpiar los datos del dataframe.
    
    Argumentos:
        base: Dataframe importado previamente y que contiene los datos a procesar.
        nombre_archivo: nombre de la fuente de datos.
    
    Retorna: 
        df_base: Dataframe con los datos seleccionados y agregados.
    
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    
    try:
        df_base = base.copy()
        #df_base = df_base.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        #df_base = df_base.applymap(lambda x: x.upper() if isinstance(x, str) else x)
        df_base = df_base.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
        df_base = df_base.apply(lambda col: col.map(lambda x: x.upper() if isinstance(x, str) else x))

        caracteres_a_eliminar=r'[|.*}$@]'
        for campo in df_base.columns:
            if campo in df_base.select_dtypes(include=['object']).columns:
                df_base[campo] = df_base[campo].astype(str).str.replace('\n', '', regex=True).str.replace('\r', '', regex=True).str.replace('\t', '', regex=True).str.replace('  ', '', regex=True).str.replace(caracteres_a_eliminar, '', regex=True)
                df_base[campo] = df_base[campo].astype(str).str.upper().str.strip()

       
    except Exception as e:
        fuentes.append(nombre_archivo)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(limpiezaCamposDataframe.__name__)
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
        if 'estrato' in df_final.columns:
            df_final=df_final.drop(columns=['estrato'])
        print(f'cantidad de registros salientes: {df_final.shape[0]}')
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'fuentes_cruda'
        nombre_tabla = 'tb_datos_crudos_transfers'
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
                
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_fuentes_transfers)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatosBD.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
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
    log_file = os.path.join(log_directory, "cargue_datos_crudos_transfers.log")
 
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
    print('Iniciando proceso de Unificacion de Transfers en Historico..\n')
    time.sleep(2)
    transfer=pd.DataFrame()
    transfer1=pd.DataFrame()
    ordenes_trabajo=pd.DataFrame()
    ordenes_trabajo1=pd.DataFrame()
    
    try:
        configurarLogging()
        id_ejecucion = generate_uuid().upper() 
        id_ejecucion_en_curso = id_ejecucion
        fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")

        df_transfers_historico = consultarTransfersHistoricos()
        #print(f'cantidad de registros historicos: {df_transfers_historico.shape[0]}')
        if df_transfers_historico is not None:
            fecha_control['fecha'] = pd.to_datetime(df_transfers_historico['fecha_procesamiento'])
            # Obtener el timestamp más reciente de cada DataFrame
            fecha_control['historico'] = fecha_control['fecha'].max()
            fecha_actual_max=fecha_control['historico'][0]

            print(f"Fecha Historica más reciente: {fecha_actual_max}")
            # Extraer días, segundos, horas, minutos de la diferencia
        else:
            print("Historico inaccesible, se usara fecha actual del sistema.")
            
        df_instaladas_crudo = consultarCrudoInstaladasUp(fecha_actual_max)
        if df_instaladas_crudo is not None:
            fecha_control_swp['fecha'] = pd.to_datetime(df_instaladas_crudo['fecha_procesamiento'])
            fecha_control=pd.concat([fecha_control,fecha_control_swp ], ignore_index=True) 
            df_instaladas_crudo=limpiezaCamposDataframe(df_instaladas_crudo,par.nombre_fuente_instaladasup)
            df_transformada=adicionRegistrosTransfers(df_instaladas_crudo)
            if df_transformada is not None:
                transfer_swp=pd.concat([transfer,df_transformada ], ignore_index=True) 
            transfer=transfer_swp
        else:
              print('No hay data cruda de instaladasUP')
        del df_instaladas_crudo

        df_digitadas_crudo = consultarCrudoDigitadasUp(fecha_actual_max)
        if df_digitadas_crudo is not None:
            fecha_control_swp['fecha'] = pd.to_datetime(df_digitadas_crudo['fecha_procesamiento'])
            fecha_control=pd.concat([fecha_control,fecha_control_swp ], ignore_index=True) 
            df_digitadas_crudo=limpiezaCamposDataframe(df_digitadas_crudo,par.nombre_fuente_digitadasup)
            df_transformada=adicionRegistrosTransfers(df_digitadas_crudo)
            if df_transformada is not None:
                    transfer_swp=pd.concat([transfer,df_transformada ], ignore_index=True) 
            transfer=transfer_swp
        else:
              print('No hay data cruda de digitadas')
        del df_digitadas_crudo

        df_instaladas_principal = consultarCrudoInstaladas(fecha_actual_max)
        if df_instaladas_principal is not None:
            fecha_control_swp['fecha'] = pd.to_datetime(df_instaladas_principal['fecha_procesamiento'])
            fecha_control=pd.concat([fecha_control,fecha_control_swp ], ignore_index=True) 
            df_instaladas_principal=limpiezaCamposDataframe(df_instaladas_principal,par.nombre_fuente_instaladas)
            df_transformada=adicionRegistrosTransfers(df_instaladas_principal)
            if df_transformada is not None:
                        transfer_swp=pd.concat([transfer,df_transformada ], ignore_index=True) 
            transfer=transfer_swp
        del df_instaladas_principal
        gc.collect()

        df_instaladas_999 = consultarCrudoInstaladas999(fecha_actual_max)
        if df_instaladas_999 is not None:
            fecha_control_swp['fecha'] = pd.to_datetime(df_instaladas_999['fecha_procesamiento'])
            fecha_control=pd.concat([fecha_control,fecha_control_swp ], ignore_index=True) 
            df_instaladas_999=limpiezaCamposDataframe(df_instaladas_999,par.nombre_fuente_instaladas999)
            df_transformada=adicionRegistrosTransfers(df_instaladas_999)
            if df_transformada is not None:
                        transfer_swp=pd.concat([transfer,df_transformada ], ignore_index=True) 
            transfer=transfer_swp
        del df_instaladas_999

        
        df_digitadas_principal=consultarCrudoDigitadas(fecha_actual_max)
        if df_digitadas_principal is not None:
            fecha_control_swp['fecha'] = pd.to_datetime(df_digitadas_principal['fecha_procesamiento'])
            fecha_control=pd.concat([fecha_control,fecha_control_swp ], ignore_index=True) 
            df_digitadas_principal=limpiezaCamposDataframe(df_digitadas_principal,par.nombre_fuente_digitadas)
            df_transformada=adicionRegistrosTransfers(df_digitadas_principal)
            if df_transformada is not None:
                        transfer_swp=pd.concat([transfer,df_transformada ], ignore_index=True) 
            transfer=transfer_swp
        del df_digitadas_principal

        
        df_canceladas_principal=consultarCrudoCanceladas(fecha_actual_max)
        if df_canceladas_principal is not None:
            fecha_control_swp['fecha'] = pd.to_datetime(df_canceladas_principal['fecha_procesamiento'])
            fecha_control=pd.concat([fecha_control,fecha_control_swp ], ignore_index=True) 
            df_canceladas_principal=limpiezaCamposDataframe(df_canceladas_principal,par.nombre_fuente_canceladas)
            df_transformada=adicionRegistrosTransfers(df_canceladas_principal)
            if df_transformada is not None:
                        transfer_swp=pd.concat([transfer,df_transformada ], ignore_index=True) 
            transfer=transfer_swp
        del df_canceladas_principal

        df_canceladas_999=consultarCrudoCanceladas999(fecha_actual_max)
        if df_canceladas_999 is not None:
            fecha_control_swp['fecha'] = pd.to_datetime(df_canceladas_999['fecha_procesamiento'])
            fecha_control=pd.concat([fecha_control,fecha_control_swp ], ignore_index=True) 
            df_canceladas_999=limpiezaCamposDataframe(df_canceladas_999,par.nombre_fuente_canceladas999)
            df_transformada=adicionRegistrosTransfers(df_canceladas_999)
            if df_transformada is not None:
                        transfer_swp=pd.concat([transfer,df_transformada ], ignore_index=True) 
            transfer=transfer_swp
        del df_canceladas_999
        #fecha_control
        
        #transfer.to_csv('transfer.csv',encoding='utf-8',index=False,mode='w')
        #transfer.drop(columns=['id_ejecucion'], inplace=True)
        #transfer1=transfer
        #transfer1.to_csv('transfer1.csv',encoding='utf-8',index=False,mode='w')
        '''
        if transfer is not None:  
              transfer.drop(columns=['id_ejecucion'], inplace=True)
              transfer1=transfer
              transfer1.to_csv('transfer1.csv',encoding='utf-8',index=False,mode='w')
        else:
              transfer1=transfer
        '''
    
          
        gc.collect()
        
        if transfer.shape[0] > 0:  
            print(f'El id de ejecucion del proceso actual es: {id_ejecucion_en_curso}')
            df_transfers_nuevo = CamposControlTransfers(transfer,fecha_inicio_date,id_ejecucion_en_curso)
            print(f'cantidad de registros entrantes: {df_transfers_nuevo.shape[0]}')
            '''
            if 'estrato' in df_transfers_nuevo.columns:
                transfer1=df_transfers_nuevo.drop(columns=['estrato'], inplace=True)
                df_transfers_nuevo=transfer1
                del transfer1
            '''    

            df_transfers_nuevo=df_transfers_nuevo.rename(columns={'id':'id_transfers'})
            #df_transfers_nuevo.to_csv('df_transfers_nuevo.csv',encoding='utf-8',index=False,mode='w')
 

            fuentes.append(par.nombre_fuentes_transfers)
            registros = len(df_transfers_nuevo)
            cantidad_registros.append(registros)
            
            if registros > 0:
                cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_fuentes_transfers,registros,par.destino_transfers,1)
                print('cargue de resumen en tabla realizado')
                cargueDatosBD(df_transfers_nuevo)
                print('cargue de datos crudos en tabla realizado')
                time.sleep(3)
           
            fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
            duracion_proceso = fecha_fin_date - fecha_inicio_date
            duracion_proceso_seg = int(duracion_proceso.total_seconds())
            actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date,duracion_proceso_seg)
        else:
            registros = 0
            cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_fuentes_transfers,registros,par.destino_transfers,1)
            print('cargue de resumen en tabla realizado')
            fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
            duracion_proceso = fecha_fin_date - fecha_inicio_date
            duracion_proceso_seg = int(duracion_proceso.total_seconds())
            actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date,duracion_proceso_seg)
            time.sleep(3)


        duracion.append(str(duracion_proceso))
        estado.append(1)
        salidaLogMonitoreo()

    except Exception as e:
        fuentes.append(par.nombre_fuentes_transfers)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        print('Error de ejecucion, valide registro en tabla de errores o logs')
        time.sleep(3)


