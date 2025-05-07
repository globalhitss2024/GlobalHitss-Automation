# %%
"""
***************************************************************************************
* CLARO  HITSS - EMPRESAS Y NEGOCIOS                                                  *
* OBJETIVO: Extración de fuentes crudas de fibra optica                               * 
*           y cargue a base de datos de forma automatica                              *
*           Comunicacion Celular S.A.- Comcel S.A\Wilmer Camargo Ochoa - Data_PCC     *
* TABLA DE INGESTA POSTGRESQL: tb_datos_crudos_fibra_optica                           *
* FECHA CREACION: 27 de Mayo de 2024                                                  *
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
#sys.path.append('C:/Users/46122499/Documents/ambiente_desarrollo/dev-empresas-negocios-env/desarrollo_notebook')
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
destino = ['tb_datos_crudos_fibra_optica']
estado = []
funcion_error = []
descripcion_error = []
id_ejecucion_en_curso = None


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
        fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
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
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,'REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP',0,par.destino_macrofo,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()
    

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
def conectarsqlServer():

    """
    Función de conexion a sql server, 

    Argumentos:
        id_ejecucion: Contiene un numero alfanumerico para creación de llaves primarias y foraneas de la base de datos
        fecha_inicio_date: Fecha de inicio del procesamiento
        fecha_fin_date: Fecha de fin del procesamiento
        duracion_proceso: Duración del procesamiento 
        
    Retorna: 
        None
    Excepciones manejadas: 
        SQLAlchemyError as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        # Establecer la conexión utilizando pyodbc
        cadena_conexion = f'DRIVER={par.driversql1};SERVER={par.hostsql};DATABASE={par.bdsql};UID={par.usuariosql};PWD={par.contrasenasql}'
         # Establecer conexión
        conn = pyodbc.connect(cadena_conexion)
        
        return conn
    
    except pyodbc.Error as e:
            if 1 in estado:
                estado.remove(1)
            if 2 not in estado:
                estado.append(2)
            funcion_error.append(conectarsqlServer.__name__)
            descripcion_error.append(str(e)[:100])
            insertarErroresDB()
            salidaLogMonitoreo()

# %%
def conectarsqlServerTemp():
    bdsqltemp = 'db_col_dwh01_temp'
    try:
        # Establecer la conexión utilizando pyodbc
        cadena_conexion = f'DRIVER={par.driversql1};SERVER={par.hostsql};DATABASE={bdsqltemp};UID={par.usuariosql};PWD={par.contrasenasql}'
        conn = pyodbc.connect(cadena_conexion)
        return conn
    except pyodbc.Error as e:
            if 1 in estado:
                estado.remove(1)
            if 2 not in estado:
                estado.append(2)
            funcion_error.append(conectarsqlServerTemp.__name__)
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
        fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
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
        credenciales = f'DSN=DenodoODBC;UID={par.usuario_denodo};PWD={par.contraseña_denodo}'
        print(credenciales)
        conn = pyodbc.connect = (credenciales)
        return conn

    except SQLAlchemyError as e:
        fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(conexion_BD.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()


# %%
def ejecutarConsultaOdbc():
    """
    Método para ejecutar una consulta y devolver los resultados en un DataFrame.
    """
    conn = conexionDenodoOdbc()
    
    try:
        #cur = conn.cursor()
        query = "SELECT * FROM vw_tbl_inteligencia_com_ventas_pymes_total_report"
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=[desc[0] for desc in cursor.description])
        print (f'cantidad de registros descargados para macro fo1: {df.shape[0]}')
        df_resultado=ordenColumnas(df,1)
        return df_resultado
    
    except pyodbc.Error as e:
        if 1 in estado:
            estado.remove(1)
        if 2 not in estado:
            estado.append(2)
        cantidad_registros.append(0)
        funcion_error.append(ejecutar_consulta.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
        return None
    finally:
        cursor.close()
        conn.close()

# %%
def ejecutarConsultaOdbc_1():
    """
    Método para ejecutar una consulta y devolver los resultados en un DataFrame.
    """
    conn = conexionDenodoOdbc()
    
    try:
        #cur = conn.cursor()
        query = "SELECT * FROM vw_tbl_inteligencia_com_ventas_pymes_total_corp_report"
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=[desc[0] for desc in cursor.description])
        df.to_csv('data_fo2-2.csv',encoding='utf-8',index=False,mode='w')
        #print (f'cantidad de registros descargados para macro fo2: {df.shape[0]}')
        df_resultado=ordenColumnas(df,2)
        return df_resultado
    except pyodbc.Error as e:
        if 1 in estado:
            estado.remove(1)
        if 2 not in estado:
            estado.append(2)
        cantidad_registros.append(0)
        funcion_error.append(ejecutar_consulta.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
        return None
    finally:
        cursor.close()
        conn.close()

# %%
def ordenColumnas(datos,num):
    df=pd.DataFrame()
    if num ==1:
        df['ID']=datos['id']
        df['ID_ORDEN_TRABAJO_PADRE']=datos['id_orden_trabajo_padre']
        df['ESTADO_ORDEN_TRABAJO']=datos['estado_orden_trabajo']
        df['ESTADO_VENTA']=datos['estado_venta']
        df['FECHA_CREACION']=datos['fecha_creacion']
        df['MES_CREACION']=datos['mes_creacion']
        df['FECHA_INICIO_FACTURACION']=datos['fecha_inicio_facturacion']
        df['FECHA_ESTADO']=datos['fecha_estado']
        df['MES_ESTADO']=datos['mes_estado']
        df['NIT']=datos['nit']
        df['RAZON_SOCIAL']=datos['cliente']
        df['ID_CLIENTE']=datos['id_cliente']
        df['CONSULTOR_BASE']=datos['consultor_base']
        df['USUARIO_GRUPO_CONSULTOR']=datos['usuario_grupo_consultor']
        df['PRODUCTO']=datos['producto']
        df['LINEA']=datos['linea']
        df['TIPO_LINEA']=datos['tipo_linea']
        df['VELOCIDAD']=datos['velocidad']
        df['CIUDAD_INCIDENTE']=datos['ciudad_incidente']
        df['PROCESO_TIPO_VENTA']=datos['proceso_tipo_venta']
        df['NUM_SERVICIOS']=datos['num_servicios']
        df['MONTO_MONEDA_LOCAL_ACTIVACION']=datos['monto_moneda_local_activacion']
        df['MONTO_MONEDA_LOCAL_CARGO_MENSUAL']=datos['monto_moneda_local_cargo_mensual']
        df['SOPORTE_PC']=datos['soporte_pc']
        df['MONTO_MONEDA_LOCAL_ARRIENDO']=datos['monto_moneda_local_arriendo']
        df['DURACION_CONTRATO']=datos['duracion_contrato']
        df['TRM_CREACION']=datos['trm_creacion']
        df['TRM_CAMBIO_ESTADO']=datos['trm_cambio_estado']
        df['MONTO_MONEDA_LOCAL_OTROS']=datos['monto_moneda_local_otros'].fillna(0)
        df['VARIACION_MONTO_MONEDA_LOCAL_MENSUAL']=datos['variacion_monto_moneda_local_mensual'].fillna(0)
        df['VARIACION_MONTO_MONEDA_LOCAL_ARRIENDO']=datos['variacion_monto_moneda_local_arriendo'].fillna(0)
        df['VARIACION_CARGO_ARRIENDO']=datos['variacion_cargo_arriendo'].fillna(0)
        datos['variacion_total_moneda_local'] = pd.to_numeric(datos['variacion_total_moneda_local'], errors='coerce').fillna(0).astype(int)
        df['VARIACION_TOTAL_MONEDA_LOCAL'] = datos['variacion_total_moneda_local']
        datos['variacion_total'] = pd.to_numeric(datos['variacion_total'], errors='coerce').fillna(0).astype(int)
        df['VARIACION_TOTAL'] = datos['variacion_total']
        df['NRO_CONTRATO']=datos['nro_contrato']
        df['ID_PROCESO_TIPO_VENTA']=datos['id_proceso_tipo_venta']
        df['CIUDAD_DESTINO']=datos['ciudad_destino']
        df['TIPO_VENTA']=datos['tipo_venta']
        df['SEGMENTO']=datos['segmento']
        df['SEGMENTO_MERCADO']=datos['segmento_mercado']
        df['NODO']=datos['nodo']
        df['DESCRIPCION']=datos['descripcion']
        df['ID_ENLACE']=datos['id_enlace']
        df['ID_TIPO']=datos['id_tipo']
        df['TIPO_ORDEN_TRABAJO']=datos['tipo_orden_trabajo']
        df['RESOLUCION1']=datos['resolucion1']
        df['RESOLUCION2']=datos['resolucion2']
        df['RESOLUCION3']=datos['resolucion3']
        df['RESOLUCION4']=datos['resolucion4']
        df['RESOLUCION_VENTA']=datos['resolucion_venta']
        df['FAMILIA']=datos['familia']
        df['CIUDAD_ORIGEN']=datos['ciudad_incidente']
        print (f'cantidad de registros reorganizados: {df.shape[0]}')
        
    if num ==2:
        df['ID']=datos['id']
        df['ID_ORDEN_TRABAJO_PADRE']=datos['ot']
        df['OTC_DIFERIDA']=datos['otc diferida']
        df['ESTADO_ORDEN_TRABAJO']=datos['estado_orden_trabajo']
        df['ESTADO_VENTA']=datos['estado_venta']
        df['FECHA_CREACION']=datos['fecha_creacion']
        df['MES_CREACION']=datos['mes_creacion']
        df['FECHA_INICIO_FACTURACION']=datos['fecha_inicio_facturacion']
        df['FECHA_ESTADO']=datos['fecha_estado']
        df['MES_ESTADO']=datos['mes_estado']
        df['NIT']=datos['nit']
        df['RAZON_SOCIAL']=datos['razon social']
        df['ID_CLIENTE']=datos['id_cliente']
        df['CONSULTOR_BASE']=datos['consultor_base']
        df['USUARIO_GRUPO_CONSULTOR']=datos['usuario_grupo_consultor']
        df['PRODUCTO']=datos['producto (servicio)']
        df['LINEA']=datos['linea']
        df['TIPO_LINEA']=datos['tipo_linea']
        df['VELOCIDAD']=datos['velocidad']
        df['CIUDAD_INCIDENTE']=datos['ciudad_incidente']
        df['PROCESO_TIPO_VENTA']=datos['proceso_tipo_venta (tipo venta)']
        df['NUM_SERVICIOS']=datos['num_servicios']
        df['MONTO_MONEDA_LOCAL_ACTIVACION']=datos['monto_moneda_local_activacion']
        df['MONTO_MONEDA_LOCAL_CARGO_MENSUAL']=datos['monto_moneda_local_cargo_mensual']
        df['SOPORTE_PC']=datos['soporte_pc']
        df['MONTO_MONEDA_LOCAL_ARRIENDO']=datos['monto_moneda_local_arriendo']
        df['DURACION_CONTRATO']=datos['duracion_contrato']
        df['TRM_CREACION']=datos['trm_creacion']
        df['TRM_CAMBIO_ESTADO']=datos['trm_cambio_estado']
        df['MONTO_MONEDA_LOCAL_OTROS']=datos['monto_moneda_local_otros'].fillna(0)
        df['VARIACION_MONTO_MONEDA_LOCAL_MENSUAL']=datos['variacion_monto_moneda_local_mensual'].fillna(0)
        df['VARIACION_MONTO_MONEDA_LOCAL_ARRIENDO']=datos['variacion_monto_moneda_local_arriendo'].fillna(0)
        df['VARIACION_CARGO_ARRIENDO']=datos['variacion_cargo_arriendo'].fillna(0)
        df['VARIACION_TOTAL_MONEDA_LOCAL']=datos['variacion_total_moneda_local'].fillna(0)
        df['VARIACION_TOTAL']=datos['variacion_total'].fillna(0)
        df['NRO_CONTRATO']=datos['nro_contrato']
        df['CIUDAD_DESTINO']=datos['ciudad_destino']
        df['TIPO_VENTA']=datos['tipo_venta']
        df['SEGMENTO']=datos['segmento']
        df['SEGMENTO_MERCADO']=datos['segmento_mercado (division)']
        df['NODO']=datos['nodo']
        df['DESCRIPCION']=datos['descripcion (observaciones)']
        df['ID_ENLACE']=datos['id_enlace']
        df['ID_TIPO']=datos['id_tipo']
        df['TIPO_ORDEN_TRABAJO']=datos['tipo_orden_trabajo']
        df['RESOLUCION1']=datos['resolucion1']
        df['RESOLUCION2']=datos['resolucion2']
        df['RESOLUCION3']=datos['resolucion3']
        df['RESOLUCION4']=datos['resolucion4']
        df['RESOLUCION_VENTA']=datos['resolucion_venta']
        df['FAMILIA']=datos['familia']
        df['ID_HECHOS_ORDEN_TRABAJO_INSTALACION']='0'
        df['CIUDAD_ORIGEN']=datos['ciudad_incidente']
        df['GRUPO_OBJETIVO']=datos['grupo_objetivo']
        df['COD_PROYECTO']='0'
        print (f'cantidad de registros reorganizados: {df.shape[0]}')

 
    return df


                

# %%
def ejecutar_consulta():
    """
    Método para ejecutar una consulta y devolver los resultados en un DataFrame.
    """
    conn = conectarsqlServer()
    
    try:
        cur = conn.cursor()
        query = """
            SELECT
                FO1.ID, 
                FO1.ID_ORDEN_TRABAJO_PADRE,
                FO2.OTC_DIFERIDA,
                FO2.ESTADO_ORDEN_TRABAJO,
                FO2.ESTADO_VENTA,
                LEFT(CONVERT(VARCHAR, FO2.FECHA_CREACION, 120), 10) AS FECHA_CREACION,
                FO2.MES_CREACION,
                LEFT(CONVERT(VARCHAR, FO2.FECHA_INICIO_FACTURACION, 120), 10) AS FECHA_INICIO_FACTURACION,
                LEFT(CONVERT(VARCHAR, FO2.FECHA_ESTADO, 120), 10) AS FECHA_ESTADO,
                FO2.MES_ESTADO,
                FO2.NIT,
                FO2.CLIENTE AS RAZON_SOCIAL,
                FO2.ID_CLIENTE,
                FO2.CONSULTOR_BASE,
                FO2.USUARIO_GRUPO_CONSULTOR,
                FO2.PRODUCTO,
                FO2.LINEA,
                FO2.TIPO_LINEA,
                FO2.VELOCIDAD,
                FO2.CIUDAD_INCIDENTE,
                FO2.PROCESO_TIPO_VENTA,
                FO2.NUM_SERVICIOS,
                FO2.MONTO_MONEDA_LOCAL_ACTIVACION,
                FO2.MONTO_MONEDA_LOCAL_CARGO_MENSUAL,
                FO2.SOPORTE_PC,
                FO2.MONTO_MONEDA_LOCAL_ARRIENDO,
                FO2.DURACION_CONTRATO,
                FO2.TRM_CREACION,
                FO2.TRM_CAMBIO_ESTADO,
                FO2.MONTO_MONEDA_LOCAL_OTROS,
                FO2.VARIACION_MONTO_MONEDA_LOCAL_MENSUAL,
                FO2.VARIACION_MONTO_MONEDA_LOCAL_ARRIENDO,
                FO2.VARIACION_CARGO_ARRIENDO,
                FO2.VARIACION_TOTAL_MONEDA_LOCAL,
                FO2.VARIACION_TOTAL,
                FO2.NRO_CONTRATO,
                FO1.ID_PROCESO_TIPO_VENTA,
                FO2.CIUDAD_DESTINO,
                FO2.TIPO_VENTA,
                FO2.SEGMENTO,
                FO2.SEGMENTO_MERCADO,
                FO2.NODO,
                FO2.DESCRIPCION,
                FO2.ID_ENLACE,
                FO2.ID_TIPO,
                FO2.TIPO_ORDEN_TRABAJO,
                FO2.RESOLUCION1,
                FO2.RESOLUCION2,
                FO2.RESOLUCION3,
                FO2.RESOLUCION4,
                FO2.RESOLUCION_VENTA,
                FO2.FAMILIA,
                FO2.CIUDAD_INCIDENTE AS CIUDAD_ORIGEN,
                FO2.GRUPO_OBJETIVO,
                FO2.COD_PROYECTO
            FROM 
                REPORTE_VENTAS_PYMES_TOTAL FO1
            INNER JOIN 
                REPORTE_VENTAS_PYMES_TOTAL_CORP FO2
            ON 
                FO1.ID_ORDEN_TRABAJO_PADRE = FO2.ID_ORDEN_TRABAJO_PADRE;
        """
        cur.execute(query)
        rows = cur.fetchall()
        columns = [column[0] for column in cur.description]

        df_sql = pd.DataFrame.from_records(rows, columns=columns)
        return df_sql

    except pyodbc.Error as e:
        if 1 in estado:
            estado.remove(1)
        if 2 not in estado:
            estado.append(2)
        cantidad_registros.append(0)
        funcion_error.append(ejecutar_consulta.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
        return None
    finally:
        cur.close()
        conn.close()

# %%
def ejecutar_consulta_tablafo1():
    """
    Método para ejecutar una consulta y devolver los resultados en un DataFrame.
    """
    conn = conectarsqlServer()
    
    try:
        cur = conn.cursor()
        query = """
            SELECT
                FO1.ID, 
                FO1.ID_ORDEN_TRABAJO_PADRE,
                FO1.ESTADO_ORDEN_TRABAJO,
                FO1.ESTADO_VENTA,
                LEFT(CONVERT(VARCHAR, FO1.FECHA_CREACION, 120), 10) AS FECHA_CREACION,
                FO1.MES_CREACION,
                LEFT(CONVERT(VARCHAR, FO1.FECHA_INICIO_FACTURACION, 120), 10) AS FECHA_INICIO_FACTURACION,
                LEFT(CONVERT(VARCHAR, FO1.FECHA_ESTADO, 120), 10) AS FECHA_ESTADO,
                FO1.MES_ESTADO,
                FO1.NIT,
                FO1.CLIENTE AS RAZON_SOCIAL,
                FO1.ID_CLIENTE,
                FO1.CONSULTOR_BASE,
                FO1.USUARIO_GRUPO_CONSULTOR,
                FO1.PRODUCTO,
                FO1.LINEA,
                FO1.TIPO_LINEA,
                FO1.VELOCIDAD,
                FO1.CIUDAD_INCIDENTE,
                FO1.PROCESO_TIPO_VENTA,
                FO1.NUM_SERVICIOS,
                FO1.MONTO_MONEDA_LOCAL_ACTIVACION,
                FO1.MONTO_MONEDA_LOCAL_CARGO_MENSUAL,
                FO1.SOPORTE_PC,
                FO1.MONTO_MONEDA_LOCAL_ARRIENDO,
                FO1.DURACION_CONTRATO,
                FO1.TRM_CREACION,
                FO1.TRM_CAMBIO_ESTADO,
                FO1.MONTO_MONEDA_LOCAL_OTROS,
                FO1.VARIACION_MONTO_MONEDA_LOCAL_MENSUAL,
                FO1.VARIACION_MONTO_MONEDA_LOCAL_ARRIENDO,
                FO1.VARIACION_CARGO_ARRIENDO,
                FO1.VARIACION_TOTAL_MONEDA_LOCAL,
                FO1.VARIACION_TOTAL,
                FO1.NRO_CONTRATO,
                FO1.ID_PROCESO_TIPO_VENTA,
                FO1.CIUDAD_DESTINO,
                FO1.TIPO_VENTA,
                FO1.SEGMENTO,
                FO1.SEGMENTO_MERCADO,
                FO1.NODO,
                FO1.DESCRIPCION,
                FO1.ID_ENLACE,
                FO1.ID_TIPO,
                FO1.TIPO_ORDEN_TRABAJO,
                FO1.RESOLUCION1,
                FO1.RESOLUCION2,
                FO1.RESOLUCION3,
                FO1.RESOLUCION4,
                FO1.RESOLUCION_VENTA,
                FO1.FAMILIA,
                FO1.CIUDAD_INCIDENTE AS CIUDAD_ORIGEN
            FROM [db_col_dwh01].[dbo].[REPORTE_VENTAS_PYMES_TOTAL_CORP] FO1
            WHERE ID_ORDEN_TRABAJO_PADRE IS NULL OR ID_ORDEN_TRABAJO_PADRE=0;
        """
        cur.execute(query)
        rows = cur.fetchall()
        columns = [column[0] for column in cur.description]

        df_sql = pd.DataFrame.from_records(rows, columns=columns)
        return df_sql

    except pyodbc.Error as e:
        if 1 in estado:
            estado.remove(1)
        if 2 not in estado:
            estado.append(2)
        cantidad_registros.append(0)
        funcion_error.append(ejecutar_consulta_tablafo1.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
        return None
    finally:
        cur.close()
        conn.close()

# %%
def extraccionDatosOnyx():
    """
    Este método carga los datos de Onyx para la extracción de Nombre y cédula.
    Argumentos:
        chFieldName 204
        chFieldName 190
        chFieldName 170 - Clase_producto
        chFieldName 171 - Servicio_componente
        chFieldName 147 - tipo_contrato
        chFieldName 189 - direccion_comercial
        chFieldName 198 - proyecto
    Retorna: 
        DataFrame de Fibra Óptica.
    Excepciones manejadas: 
        None
    """
    conn_server = conectarsqlServerTemp()
    if conn_server is None:
        return None  # Manejar la falta de conexión adecuadamente

    try:
        import datetime
        fecha_actual = datetime.datetime.now()
        primer_dia_mes_actual = fecha_actual.replace(day=1)
        fecha_formateada = primer_dia_mes_actual.strftime("%Y-%m-%d 00:00:00.000")
        
        consulta_sql = f""" 
        SELECT 
            chFieldName,
            iSystemId as id_orden_trabajo_padre,
            vchDataValue as nombre_consultor,
            dtUpdateDate
            FROM [db_col_dwh01_temp].[dbo].[TEMP_EXPANSION_DATA]
            WHERE (chFieldName LIKE '%190%')
            --AND dtUpdateDate >= '{fecha_formateada}'
            --AND DATALENGTH(vchDataValue) > 3;
        """

        df_onyx = pd.read_sql(consulta_sql, conn_server)
        

        # Ejecutar la consulta y guardar los resultados en un DataFrame
        if not df_onyx.empty:     
            if 1 not in estado:
                estado.append(1)
        #print(f'cantidad de registros de Onix: {df_onyx.shape[0]}')
        return df_onyx  # Aquí devolvemos el DataFrame

    except Exception as e:
        cantidad_registros.append(0)
        if 1 in estado:
            estado.remove(1)
        if 2 not in estado:
            estado.append(2)
        funcion_error.append(extraccionDatosOnyx.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

    finally:
        conn_server.close()

# %%
def extraccionDatosOnyxcolumnasextra():
    """
    Este método carga los datos de Onyx para la extracción de Nombre y cédula.
    Argumentos:
        None
    Retorna: 
        DataFrame de Fibra Óptica.
    Excepciones manejadas: 
        None
    """
    conn_server = conectarsqlServerTemp()
    if conn_server is None:
        return None  # Manejar la falta de conexión adecuadamente

    try:
        consulta_sql = """
        SELECT 
            a.[ITABLEID],
            a.[CHFIELDNAME],
            a.[ISYSTEMID] as id_orden_trabajo_padre,
            a.[ISITEID] AS a_ISITEID,
            b.[ISITEID] AS b_ISITEID,
            a.[VCHDATAVALUE],
            c.[VCHFIELDCAPTION] as nombre_columna,
            b.[vchParameterDesc] as dato_columna,
            a.[CHINSERTBY],
            a.[DTINSERTDATE],
            a.[CHUPDATEBY],
            a.[DTUPDATEDATE]
        FROM 
            [db_col_dwh01_temp].[dbo].[TEMP_EXPANSION_DATA] a
        INNER JOIN 
            [db_col_dwh01_temp].[dbo].[TEMP_REFERENCE_PARAMETERS] b
            ON CASE 
                WHEN ISNUMERIC(a.[VCHDATAVALUE]) = 1 AND 
                        a.[VCHDATAVALUE] NOT LIKE '%[^0-9]%' AND
                        LEN(a.[VCHDATAVALUE]) <= 19 
                THEN CAST(a.[VCHDATAVALUE] AS BIGINT)
                ELSE NULL
            END = b.[iParameterId]
        INNER JOIN 
            [db_col_dwh01_temp].[dbo].[TEMP_REFERENCE_FIELDS] c
            ON a.[CHFIELDNAME] = c.[CHFIELDNAME]
        WHERE (a.[CHFIELDNAME] LIKE '%198%' OR a.[CHFIELDNAME] LIKE '%189%' OR a.[CHFIELDNAME] LIKE '%169%' OR a.[CHFIELDNAME] LIKE '%170%' OR a.[CHFIELDNAME] LIKE '%171%' 
            OR a.[CHFIELDNAME] LIKE '%147%')
        ORDER BY 
            a.[DTUPDATEDATE] DESC;
        """

        df_onyx = pd.read_sql(consulta_sql, conn_server)
        
        if df_onyx.empty:
            return df_onyx  # Retornar el DataFrame vacío si no hay datos
        
        # Pivotar el DataFrame para obtener las columnas adicionales
        df_pivoted = df_onyx.pivot_table(
            index=['id_orden_trabajo_padre'], 
            columns='nombre_columna', 
            values='dato_columna', 
            aggfunc='first'
        ).reset_index()
        
        # Renombrar columnas para coincidir con la tabla destino
        column_mapping = {
            'Proyecto': 'proyecto',
            'Dirección Comercial': 'direccion_comercial',
            'Familia': 'familia_1',
            'Clases Producto': 'clase_producto',
            'Servicio/Componente': 'servicio_componente',
            'Tipo de Contrato': 'tipo_contrato'
        }
        df_pivoted.rename(columns=column_mapping, inplace=True)
        
        # Añadir columnas adicionales que sean necesarias con valores nulos
        for col in ['proyecto', 'direccion_comercial', 'familia_1', 'clase_producto', 'servicio_componente','tipo_contrato']:
            if col not in df_pivoted.columns:
                df_pivoted[col] = None
        
        return df_pivoted

    except Exception as e:
        cantidad_registros.append(0)
        if 1 in estado:
            estado.remove(1)
        if 2 not in estado:
            estado.append(2)
        funcion_error.append(extraccionDatosOnyxcolumnasextra.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

    finally:
        conn_server.close()


# %%
def comparar_id_orden_trabajo_padre():
    try:
        # Obtener los DataFrames de las funciones
        df_onyx = extraccionDatosOnyx()
        df_onyx_columnas = extraccionDatosOnyxcolumnasextra()
        df_sql = ejecutarConsultaOdbc()
        #df_sql = ejecutar_consulta()

        
        # Verificar si ambos DataFrames fueron obtenidos correctamente
        if df_onyx is None or df_sql is None or df_onyx_columnas is None:
            print("No se pudo obtener uno o ambos DataFrames de Onyx o la consulta SQL.")
            return None
        
        # Convertir los valores de id_orden_trabajo_padre a conjunto para la comparación
        onyx_ids = set(df_onyx['id_orden_trabajo_padre'])
        sql_ids = set(df_sql['ID_ORDEN_TRABAJO_PADRE'])
        
        # Encontrar los ids que están en ambos conjuntos (intersección)
        ids_comunes = onyx_ids.intersection(sql_ids)


        
        # Crear DataFrame con los registros de df_onyx que tienen ids_comunes
        df_ids_comunes = df_onyx[df_onyx['id_orden_trabajo_padre'].isin(ids_comunes)]

        
        # Integrar los datos adicionales de Onyx a df_ids_comunes usando mergees
        df_ids_comunes = pd.merge(df_ids_comunes, df_onyx_columnas, on='id_orden_trabajo_padre', how='left')
        
        return df_ids_comunes
    
    except Exception as e:
        cantidad_registros.append(0)
        if 1 in estado:
            estado.remove(1)
        if 2 not in estado:
            estado.append(2)
        funcion_error.append(comparar_id_orden_trabajo_padre.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
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
        fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
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
        fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
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
    log_file = os.path.join(log_directory, "cargue_datos_crudos_fibra_optica.log")

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
        nombre_tabla = 'tb_datos_crudos_fibra_optica'
        #print('cargando datos en base de datos principal')
        #df_final.to_csv('data_base.csv',encoding='utf-8',index=False,mode='w')
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
       
        
    except SQLAlchemyError as e:
        fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
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
def obtener_estado_orden_trabajo():
    """
    Consulta en la tabla de Postgress el id del estado orden trabajo
    
    Argumentos:
        df: DataFrame que contiene la columna ESTADO_ORDEN_TRABAJO.
    
    Retorna: 
        df: df_estado de la columna
    """
    conn = conexion_BD()
    if conn:
        try:
            query = "SELECT id_fo_estado, estado_orden_trabajo FROM fuentes_cruda.tb_fo_estado_orden_trabajo"
            df_estado = pd.read_sql(query, conn)
            return df_estado
        except SQLAlchemyError as e:
            fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
            cantidad_registros.append(0)
            estado.append(2)
            funcion_error.append(obtener_estado_orden_trabajo.__name__)
            descripcion_error.append(str(e)[:100])
            insertarErroresDB()
            salidaLogMonitoreo()
        finally:
            conn.close()
    else:
        return None

# %%
def obtener_estado_venta():
    """
    Consulta en la tabla de Postgress el id del estado venta
    
    Argumentos:
        df: DataFrame que contiene la columna ESTADO_VENTA.
    
    Retorna: 
        df: df_estado de la columna
    """
    conn = conexion_BD()
    if conn:
        try:
            query = "SELECT id_fo_estado_venta, estado_venta, fecha_creacion, fecha_modificacion, id_estado_registro FROM fuentes_cruda.tb_fo_estado_venta"
            df_estado = pd.read_sql(query, conn)
            return df_estado
        except SQLAlchemyError as e:
            fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
            cantidad_registros.append(0)
            estado.append(2)
            funcion_error.append(obtener_estado_venta.__name__)
            descripcion_error.append(str(e)[:100])
            insertarErroresDB()
            salidaLogMonitoreo()
        finally:
            conn.close()
    else:
        return None

# %%
def asignar_id_estados(df):
    """
    Asigna el ID correspondiente de la tabla tb_fo_estado_orden_trabajo a cada estado en la columna ESTADO_ORDEN_TRABAJO.
    
    Argumentos:
        df: DataFrame que contiene la columna ESTADO_ORDEN_TRABAJO.
    
    Retorna: 
        df: DataFrame con la columna id_estado_orden_trabajo actualizada.
    """
    try:
        # Obtener el DataFrame de estados desde la tabla ESTADOS_ORDEN_TRABAJO
        df_estados = obtener_estado_orden_trabajo()
        
        # Crear un diccionario de mapeo de estados (estado -> id_fo_estado)
        estado_dict = dict(zip(df_estados['estado_orden_trabajo'], df_estados['id_fo_estado']))
        
        # Convertir todos los valores en la columna ESTADO_ORDEN_TRABAJO a mayúsculas
        df['ESTADO_ORDEN_TRABAJO'] = df['ESTADO_ORDEN_TRABAJO'].str.upper()
        
        # Asignar el ID correspondiente a cada estado en la columna ESTADO_ORDEN_TRABAJO
        df['id_estado_orden_trabajo'] = df['ESTADO_ORDEN_TRABAJO'].map(estado_dict)
       
        return df['id_estado_orden_trabajo']

    except Exception as e:
        fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(asignar_id_estados.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
def asignar_id_estados_venta(df):
    """
    Asigna el ID correspondiente de la tabla tb_fo_estado_venta a cada estado en la columna ESTADO_VENTA.
    
    Argumentos:
        df: DataFrame que contiene la columna ESTADO_VENTA.
    
    Retorna: 
        df: DataFrame con la columna id_estado_venta actualizada.
    """
    try:
        # Obtener el DataFrame de estados desde la tabla ESTADOS_VENTA
        df_estados = obtener_estado_venta()
        
        # Crear un diccionario de mapeo de estados (estado_venta -> id_fo_estado_venta)
        estado_dict = dict(zip(df_estados['estado_venta'], df_estados['id_fo_estado_venta']))
        
        # Convertir todos los valores en la columna ESTADOS_VENTA a mayúsculas
        df['ESTADO_VENTA'] = df['ESTADO_VENTA'].str.upper()
        
        # Asignar el ID correspondiente a cada estado en la columna ESTADOS_VENTA
        df['id_estado_venta'] = df['ESTADO_VENTA'].map(estado_dict)
       
        return df['id_estado_venta']

    except Exception as e:
        fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(asignar_id_estados_venta.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
def seleccionCamposfibraoptica(df, df_onyx, df_resultado_fo1,df_historico, fecha_inicio_date, id_ejecucion):
    """
    Función que se encarga de seleccionar los datos correspondientes de la base de fibra óptica
    y crea campos adicionales necesarios para el control de los datos.

    *** se realiza el llenado del id nulo o vacio con el numero cero (0) debido a la restriccion de no nulo de la tabla de crudos ***
    
    Argumentos:
        df: DataFrame importado previamente y que contiene los datos a procesar.
        df_onyx: DataFrame con la información de Onyx.
        df_resultado_fo1: DataFrame con la información adicional de FO1.
        fecha_inicio_date: Fecha de inicio del procesamiento.
        id_ejecucion: Contiene un número alfanumérico para creación de llaves primarias y foráneas de la base de datos.
    
    Retorna: 
        df_selected: DataFrame con los datos seleccionados y agregados.
    """
    try:
        df_base = df.copy()
        df_fo1 = df_resultado_fo1.copy()

        # Combinación de los datos de df y df_fo1
        df_base = pd.concat([df_base, df_fo1], ignore_index=True)

        df_base['id_macrofo'] = [generate_uuid().upper() for _ in range(len(df_base))]
        df_base['id_ejecucion'] = id_ejecucion
        df_base['id_estado'] = 1
        df_base['id'] = df_base['ID'].fillna(0)
        df_base['id_orden_trabajo_padre'] = df_base['ID_ORDEN_TRABAJO_PADRE'] 
        df_base['otc_diferida'] = df_base['OTC_DIFERIDA'] 
        df_base['estado_orden_trabajo'] = df_base['ESTADO_ORDEN_TRABAJO'].str.upper() 
        df_base['estado_venta'] = df_base['ESTADO_VENTA'].str.upper() 
        df_base['fecha_creacion'] = df_base['FECHA_CREACION'] 
        df_base['mes_creacion'] = df_base['MES_CREACION'] 
        df_base['fecha_inicio_facturacion'] = df_base['FECHA_INICIO_FACTURACION'] 
        df_base['fecha_estado'] = df_base['FECHA_ESTADO'] 
        df_base['mes_estado'] = df_base['MES_ESTADO'] 
        df_base['nit'] = df_base['NIT'] 
        df_base['razon_social'] = df_base['RAZON_SOCIAL'] 
        df_base['id_cliente'] = df_base['ID_CLIENTE'] 
        df_base['consultor_base'] = df_base['CONSULTOR_BASE'] 
        df_base['usuario_grupo_consultor'] = df_base['USUARIO_GRUPO_CONSULTOR'] 
        df_base['producto'] = df_base['PRODUCTO'] 
        df_base['linea'] = df_base['LINEA'] 
        df_base['tipo_linea'] = df_base['TIPO_LINEA'] 
        df_base['velocidad'] = df_base['VELOCIDAD'] 
        df_base['ciudad_incidente'] = df_base['CIUDAD_INCIDENTE'].str.upper() 
        df_base['proceso_tipo_venta'] = df_base['PROCESO_TIPO_VENTA'] 
        df_base['num_servicios'] = df_base['NUM_SERVICIOS'] 
        df_base['monto_moneda_local_activacion'] = df_base['MONTO_MONEDA_LOCAL_ACTIVACION'].fillna(0)
        df_base['monto_moneda_local_cargo_mensual'] = df_base['MONTO_MONEDA_LOCAL_CARGO_MENSUAL'].fillna(0)
        df_base['soporte_pc'] = df_base['SOPORTE_PC'] 
        df_base['monto_moneda_local_arriendo'] = df_base['MONTO_MONEDA_LOCAL_ARRIENDO'] 
        df_base['duracion_contrato'] = df_base['DURACION_CONTRATO'] 
        df_base['trm_creacion'] = df_base['TRM_CREACION'] 
        df_base['trm_cambio_estado'] = df_base['TRM_CAMBIO_ESTADO'] 
        df_base['monto_moneda_local_otros'] = df_base['MONTO_MONEDA_LOCAL_OTROS'].fillna(0)
        df_base['variacion_monto_moneda_local_mensual'] = df_base['VARIACION_MONTO_MONEDA_LOCAL_MENSUAL'].fillna(0)
        df_base['variacion_monto_moneda_local_arriendo'] = df_base['VARIACION_MONTO_MONEDA_LOCAL_ARRIENDO'].fillna(0)
        df_base['variacion_cargo_arriendo'] = df_base['VARIACION_CARGO_ARRIENDO'].fillna(0)
        df_base['VARIACION_TOTAL_MONEDA_LOCAL'] = pd.to_numeric(df_base['VARIACION_TOTAL_MONEDA_LOCAL'], errors='coerce').fillna(0).astype(int)
        df_base['variacion_total_moneda_local'] = df_base['VARIACION_TOTAL_MONEDA_LOCAL'].fillna(0)
        df_base['VARIACION_TOTAL'] = pd.to_numeric(df_base['VARIACION_TOTAL'], errors='coerce').fillna(0).astype(int)
        df_base['variacion_total'] = df_base['VARIACION_TOTAL']
        df_base['nro_contrato'] = df_base['NRO_CONTRATO'] 
        df_base['id_proceso_tipo_venta'] = df_base['ID_PROCESO_TIPO_VENTA'].str.upper() 
        df_base['ciudad_destino'] = df_base['CIUDAD_DESTINO'].str.upper() 
        df_base['tipo_venta'] = df_base['TIPO_VENTA'].str.upper() 
        df_base['segmento'] = df_base['SEGMENTO'] 
        df_base['segmento_mercado'] = df_base['SEGMENTO_MERCADO'].str.upper() 
        df_base['nodo'] = df_base['NODO'] 
        df_base['descripcion'] = df_base['DESCRIPCION'].str.upper() 
        df_base['id_enlace'] = df_base['ID_ENLACE'] 
        df_base['id_tipo'] = df_base['ID_TIPO'].str.lstrip('$')
        df_base['tipo_orden_trabajo'] = df_base['TIPO_ORDEN_TRABAJO'].str.lstrip('$')
        df_base['resolucion1'] = df_base['RESOLUCION1'].str.upper() 
        df_base['resolucion2'] = df_base['RESOLUCION2'].str.upper() 
        df_base['resolucion3'] = df_base['RESOLUCION3'].str.upper() 
        df_base['resolucion4'] = df_base['RESOLUCION4'].str.upper() 
        df_base['resolucion_venta'] = df_base['RESOLUCION_VENTA'].str.upper() 
        df_base['familia'] = df_base['FAMILIA'].str.upper() 
        df_base['ciudad_origen'] = df_base['CIUDAD_ORIGEN'].str.upper() 
        df_base['grupo_objetivo'] = df_base['GRUPO_OBJETIVO'].str.upper()
        df_base['cod_proyecto'] = df_base['COD_PROYECTO'].str.upper()
        
        # Comprobar si las columnas existen en df_onyx antes de realizar el merge
        columnas_necesarias = ['id_orden_trabajo_padre', 'nombre_consultor', 'clase_producto', 'servicio_componente', 'direccion_comercial', 'proyecto', 'tipo_contrato', 'familia_1']
        for col in columnas_necesarias:
            if col not in df_onyx.columns:
                df_onyx[col] = ''

        # Convertir las columnas necesarias a mayúsculas
        df_onyx[columnas_necesarias] = df_onyx[columnas_necesarias].apply(lambda x: x.str.upper() if x.dtype == "object" else x)

        df_onyx['nombre_consultor'] = df_onyx['nombre_consultor'].replace('0', None)
        df_base['cod_proyecto'] = df_base['cod_proyecto'].apply(lambda x: None if x in ['0', '.', 'NA', 'N/A', 'NO', 'n/a', '00', 'N','..','N.A','0.'] else x)

        # Realizar el merge para agregar columnas de df_onyx
        df_base = pd.merge(df_base, df_onyx[columnas_necesarias], on='id_orden_trabajo_padre', how='left')

        # Obtener el mapeo de estados de orden de trabajo
        df_base['id_estado_orden_trabajo'] = asignar_id_estados(df_base)
        df_base['id_estado_venta'] = asignar_id_estados_venta(df_base)

        # la columna 'id_estado_orden_trabajo' existe
        if 'id_estado_orden_trabajo' not in df_base.columns:
            df_base['id_estado_orden_trabajo'] = None

        # Asignar el valor predeterminado 13 si 'id_estado_orden_trabajo' es nulo
        df_base['id_estado_orden_trabajo'] = df_base['id_estado_orden_trabajo'].fillna(13)

        df_base['id_estado_venta'] = df_base['id_estado_venta'].fillna(6)

        df_base['llave_compuesta'] = (
        df_base['id'].astype(str) + '-' +
        df_base['id_orden_trabajo_padre'].fillna('').astype(str) + '-' +
        df_base['producto'].fillna('').astype(str))

        df_merged = pd.merge(df_base, df_historico[['llave_compuesta']], on='llave_compuesta', how='outer', indicator=True)
        df_nuevos = df_merged[df_merged['_merge'] == 'left_only'].copy()
        df_nuevos.drop(columns=['_merge'], inplace=True)
        df_nuevos.drop(columns=['llave_compuesta'], inplace=True)

        df_base = pd.concat([df_nuevos], ignore_index=True)


        # Selección de columnas
        columnas_ordenadas = [
            'id_macrofo', 'id_ejecucion', 'id', 'id_orden_trabajo_padre', 'otc_diferida', 
            'id_estado_orden_trabajo', 'id_estado_venta', 'fecha_creacion', 'mes_creacion', 'fecha_inicio_facturacion', 'fecha_estado', 
            'mes_estado', 'nit', 'razon_social', 'id_cliente', 'consultor_base', 
            'usuario_grupo_consultor', 'producto', 'linea', 'tipo_linea', 'velocidad', 
            'ciudad_incidente', 'proceso_tipo_venta', 'num_servicios', 'monto_moneda_local_activacion', 
            'monto_moneda_local_cargo_mensual', 'soporte_pc', 'monto_moneda_local_arriendo', 
            'duracion_contrato', 'trm_creacion', 'trm_cambio_estado', 'monto_moneda_local_otros', 
            'variacion_monto_moneda_local_mensual', 'variacion_monto_moneda_local_arriendo', 
            'variacion_cargo_arriendo', 'variacion_total_moneda_local', 'variacion_total', 
            'nro_contrato', 'id_proceso_tipo_venta', 'ciudad_destino', 'tipo_venta', 
            'segmento', 'segmento_mercado', 'nodo', 'descripcion', 'id_enlace', 'id_tipo', 
            'tipo_orden_trabajo', 'resolucion1', 'resolucion2', 'resolucion3', 
            'resolucion4', 'resolucion_venta', 'familia', 'ciudad_origen', 'grupo_objetivo', 
            'nombre_consultor', 'cod_proyecto', 'clase_producto', 'tipo_contrato', 'servicio_componente',
            'direccion_comercial', 'proyecto', 'familia_1', 'id_estado'
        ]

        # Filtrar columnas existentes
        columnas_seleccionadas = [col for col in columnas_ordenadas if col in df_base.columns]
        df_selected = df_base[columnas_seleccionadas]
        df_selected['fecha_inicio_procesamiento'] = pd.to_datetime(fecha_inicio_date)
        

        return df_selected

    except Exception as e:
        fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(seleccionCamposfibraoptica.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
        raise e  # Re-raise the exception to handle it outside the function


def consultarfoHistorico():
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
        sql_consulta = "Select * \
                    from fuentes_cruda.tb_datos_crudos_fibra_optica"
        df_fo_historico = pd.read_sql(sql_consulta, engine)
    
        return df_fo_historico
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_instaladas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarInstaladasHistorico.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()



# %%
if __name__ == "__main__":
    try:
        # Configuración del logging, generación del UUID de ejecución y consulta a la base de datos
        configurarLogging()
        id_ejecucion = generate_uuid().upper()
        fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")

        df_resultado_denodo=ejecutarConsultaOdbc_1()
        print(f'cantidad de registros FO2: {df_resultado_fo.shape[0]}')
        #df_resultado_denodo.to_csv('data_fo2.csv',encoding='utf-8',index=False,mode='w') todo mayuscula lore
        df_resultado_fo1=ejecutarConsultaOdbc()
        print(f'cantidad de registros FO1: {df_resultado_fo1.shape[0]}')
        #df_resultado_fo1.to_csv('data_fo1.csv',encoding='utf-8',index=False,mode='w') todo mayuscula lore
        df_onyx = comparar_id_orden_trabajo_padre()
        print(f'cantidad de registros Onyx: {df_onyx.shape[0]}')

        df_historicoFo=consultarfoHistorico()
        
        if df_resultado_denodo is not None:
            registros = len(df_resultado_denodo)
        
            if registros > 0:
                # Realiza la selección de campos y agrega información adicional
                df_base = seleccionCamposfibraoptica(df_resultado_denodo, df_onyx, df_resultado_fo1,df_historicoFo, fecha_inicio_date, id_ejecucion)
                if df_base is not None:
                    #print('cargando de resumen de datos en base de datos principal')
                    df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, 'REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP', registros, 'tb_datos_crudos_fibra_optica', 1)
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
        fuentes.append('REPORTE_VENTAS_PYMES_TOTAL, REPORTE_VENTAS_PYMES_TOTAL_CORP')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        


