# %%
"""
***************************************************************************************
* CLARO  HITSS - EMPRESAS Y NEGOCIOS                                                  *
* OBJETIVO: Automatización proceso altas                                                    *                                                                            *
* TABLA DE INGESTA Dendodo: xxxxx                                      *
* FECHA CREACION: 27 de Noviembre de 2024                                                  *
* ELABORADO POR: Johana Perez Montoya                                                  *
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
from datetime import datetime, timedelta
import pyodbc
import sys
from pyflowchart import Flowchart
sys.path.append('C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\desarrollo_notebook')
#sys.path.append('C:/ambiente_desarrollo/dev-empresas-negocios-env/esarrollo_notebook')
#sys.path.append('C:/Users/46122499/Documents/ambiente_desarrollo/dev-empresas-negocios-env/desarrollo_notebook')
import parametros_desarrollo as par
import uuid
import xlsxwriter
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
import psycopg2
import logging
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# %%

#VARIABLES GLOBALES
fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
fecha_actual = datetime.today().date()
duracion = []
fuentes = []
cantidad_registros = []
destino = []
estado_error = []
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
    logging.info(f"Estado: {estado_error}")
    logging.info("Lugar errores: " + ' | '.join(map(str, funcion_error)))
    logging.info("Descripción errores: " + ' | '.join(map(str, descripcion_error)))
    if estado_error[0] == 1:
        logging.info("Ejecución exitosa")
    logging.info("------------------------------------------------------------------")

# %%


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
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
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
            'id_estado': estado_error,
            'funcion_error': funcion_error,
            'descripcion_error': descripcion_error
        })
        
        conexion_errores = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial}')
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_errores_cargue'
        errores.to_sql(nombre_tabla, con=conexion_errores, schema=nombre_esquema, if_exists='append', index=False)
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,par.nov,par.can,par.planta,par.asig,par.fo,par.hfc,par.vens,0,par.destino_altas,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
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
        print(e)
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(conexion_BD.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()                  

# %%
def conexion_BD_produccion():
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
            database=par.bd_inteligencia_produccion,
            user=par.usuario,
            password=par.contrasena
        )
        return conn

    except SQLAlchemyError as e:
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
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
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(actualizarFechaFinProcesamiento.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
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
    
        cadena=f'DSN=DenodoODBC;UID={par.usuario_denodo};PWD={par.contraseña_denodo}'
        conn = pyodbc.connect(cadena)
        return conn

    
    except SQLAlchemyError as e:
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(conexion_BD.__name__)
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
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(generate_uuid.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def ejecutarConsultaOdbcUpselling():
    """
    Método para ejecutar una consulta y devolver los resultados en un DataFrame.
    
    Argumentos:
        None
    
    Retorna:
        DataFrame: Contiene los resultados de la consulta.
    
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    try:
        #mes en curso
        fecha_final = fecha_actual - timedelta(days=1)
        fecha_inicial = fecha_final.replace(day=1)
     
        #del primero al 30 o 31
        #fecha_final = fecha_actual.replace(day=1) - timedelta(days=1)
        #fecha_inicial = fecha_final.replace(day=1)
    
        conn = conexionDenodoOdbc()
        
        query = f"""
        SELECT 
        Upselling."FECHA INICIO FACTURACION ACTUAL" AS "FECHA",
        Upselling."ID CLIENTE",
        Upselling."NIT" AS "NIT_CLIENTE",
        Upselling."COMPANIA" AS "RAZON_SOCIAL",
        Upselling."GRUPO OBJETIVO",
        Upselling."INCIDENTE ACTUAL" AS "OT",
        Upselling."TIPO INCIDENTE ACTUAL",
        Upselling."ENLACE",
        Upselling."LINEA" AS "FAMILIA",
        Upselling."TIPO" AS "ESTADO DEL MRC",
        Upselling."SEGMENTO CLIENTE" as "SEGMENTO",
        Upselling."FECHA CIERRE NOVEDAD ACTUAL" AS "FECHA DE ESTADO DE LA OT",
        Upselling."CONSULTOR COMERCIAL" AS "NOMBRE CONSULTOR",
        Upselling."CONSULTOR COMERCIAL" AS "NOMBRE_CONSULTOR_ALTA",
        Upselling."CAMBIO TOTAL RECURRENTE PESOS",
        Upselling."CAMBIO TOTAL"AS "TOTAL_CIFRA_DE_ALTAS_(Total mensualidad)",
        Upselling."TIPO NOVEDAD ACTUAL",
        Upselling."FECHA FINAL",
        Upselling."FECHA INICIAL",
        'UPSELLING' AS "FUENTE"

        FROM db_dwh_corporativo.vw_tbl_facturacion_novedades_report as Upselling
        WHERE "FECHA FINAL" = '{fecha_final}' AND "FECHA INICIAL" = '{fecha_inicial}'
        AND "SEGMENTO CLIENTE" IN ('ESTRATEGICA', 'GRANDES', 'Gobierno')  
        AND "TIPO INCIDENTE ACTUAL" = 'OT'  
        AND "COMPANIA" NOT IN ('TELMEX COLOMBIA IT', 'TELMEX COLOMBIA S.A.', 'COMCEL')  
        AND "GRUPO OBJETIVO" NOT IN ('CABLERAS TELMEX')
        AND "CAMBIO TOTAL RECURRENTE PESOS" >= 0
        --AND Upselling."TIPO" IN ('SIN IMPACTO', 'UPSELLING')
        AND "TIPO NOVEDAD ACTUAL" IN (
        '$ADICION / RETIRO DE NUMEROS',
        '$ADICION DE CUENTAS DE CORREO',
        '$ADICIÓN DE EQUIPOS',
        '$CAMBIO DE EQUIPOS',
        '$CAMBIO DE SERVICIO',
        '$CAMBIO DE VELOCIDAD(AMPLIACIÓN - REDUCCIÓN)',
        '$CAMBIO PLAN PYMES',
        '$CAMBIO TIPO ACCESO, SERVICIO Y AMPLIACIÓN',
        '$CAMBIOS SERVICIOS SUPLEMENTARIOS',
        '$MODIFICACIÓN NUMERACIÓN PÚBLICA',
        '$NOVEDAD ALIANZA',
        '$NOVEDADES SOLUCIONES ADMINISTRADAS',
        '$RETIRO DE EQUIPOS',
        '$TRASLADO EXTERNO',
        '$TRASLADO EXTERNO PYMES',
        '$TRASLADO INTERNO',
        '$TRASLADO INTERNO PYMES',
        'SOLICITUD DIR.IP FIJAS.PYMES'
    )
        CONTEXT('cache_wait_for_load' = 'true') TRACE;
            """
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        df_resultado = pd.DataFrame.from_records(rows, columns=[desc[0] for desc in cursor.description])
        df_resultado = df_resultado

        cursor.close()
        conn.close()
        print(df_resultado.dtypes)
        df_resultado.to_excel("resultado_consulta_UPSELLING.xlsx", index=False)
        return df_resultado

    except Exception as e:
            print(e)
            fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
            cantidad_registros.append(0)
            estado_error.append(2)
            funcion_error.append(ejecutarConsultaOdbcUpselling.__name__)
            descripcion_error.append(str(e)[:100])
            insertarErroresDB()
            salidaLogMonitoreo()         
         
           
                                                                        

# %%

def ejecutarConsultaOdbcSltasNew():
    """
    Función encargada de ejecutar una consulta SQL en una base de datos mediante una conexión ODBC, 
    obtener datos relacionados con órdenes de trabajo y retornarlos como un DataFrame.
    
    Argumentos:
        None
    
    Retorna:
        DataFrame: Contiene los resultados de la consulta.
    
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        conn = conexionDenodoOdbc()

        #mes en curso
        fecha_final = fecha_actual - timedelta(days=1)
        fecha_inicial = fecha_final.replace(day=1)

      
        query = f"""
      SELECT 
    new."CIUDAD_2" as "CIUDAD",
    new."NOMBRE_CLIENTE" AS "RAZON_SOCIAL",
    new."MONTO_MONEDA_LOCAL_CARGO_MENSUAL" AS "TOTAL_CIFRA_DE_ALTAS_(Total mensualidad)",
    new."SEGMENTO",
    NEW."ID_ENLACE" AS "ENLACE",
    new."NIT" as "NIT_CLIENTE",
    new."ORDEN_TRABAJO",
    new."NRO_INCIDENTE_ONYX" AS "ID",
    new."NRO_OT_ONYX" AS "OT",
    new."SERVICIO_2" AS "SERVICIO_ONYX",
    new."FAMILIA_2" AS "FAMILIA",
    new."ESTADO_ORDEN_TRABAJO",
    new."FECHA_REALIZACION" AS "FECHA DE ESTADO DE LA OT",
    new."CONSULTOR_COMERCIAL" AS "NOMBRE CONSULTOR",
    new."CONSULTOR_COMERCIAL" AS "NOMBRE_CONSULTOR_ALTA",
    new."GRUPO_OBJETIVO",
    'NEW' as "FUENTE"
    FROM db_dwh_corporativo.vw_tbl_imp_ct_otp_cerradas_cancelada_report as new
    WHERE "SEGMENTO" IN ('ESTRATEGICA','GRANDES')
    AND "ESTADO_ORDEN_TRABAJO" = 'Cerrada'
    AND new."ORDEN_TRABAJO" LIKE '%Instalación%'
    AND "SERVICIO_2" NOT IN ('CLARO T-Soluciona básico')
    AND NOT (
            "ORDEN_TRABAJO" LIKE '%Instalación Grandes Proyectos%' 
            OR "ORDEN_TRABAJO" LIKE '%Instalación Pymes%' 
            OR "ORDEN_TRABAJO" LIKE '%Instalación Servicios Transmisión%')

    AND new."GRUPO_OBJETIVO" NOT LIKE '%TELMEX%'
    AND new."GRUPO_OBJETIVO" NOT LIKE '%AMERICA MOVIL (INTERCOMPANY)'
    AND "FECHA_REALIZACION" BETWEEN '{fecha_inicial}' AND '{fecha_final}';
        """
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        df_resultado = pd.DataFrame.from_records(rows, columns=[desc[0] for desc in cursor.description])
        df_resultado = df_resultado

        cursor.close()
        conn.close()
        #print(df_resultado.dtypes)
        # df_resultado.to_excel("resultado_consulta_new.xlsx", index=False)
        #
        return df_resultado

    except Exception as e:
        print(e)
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(ejecutarConsultaOdbcSltasNew.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
ejecutarConsultaOdbcSltasNew()                                                             

# %% [markdown]
# 

# %%
def ejecutarConsultaInteligencia():
    """
    Función encargada de ejecutar una consulta SQL en la base de datos, extraer datos relevantes
    sobre la planta comercial  y retornarlos en un DataFrame.
    
    Argumentos:
        None
    
    Retorna:
        DataFrame: Contiene los resultados de la consulta.
    
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    conn = conexion_BD()
    try:
        query = """
        SELECT 
        tdcpc.identificacion AS "DOCUMENTO_CONSULTOR",
        tdcpc.nombre_completo AS "NOMBRE CONSULTOR",
        tpe2.estado  as "ESTADO_CONSULTOR",
        tpc.cargo_actual AS "CARGO_ACTUAL",
        tpj.identificacion AS "DOCUMENTO_JEFE",
        tpj.nombre AS "NOMBRE_JEFE", 
        tpcd.identificacion  as "DOCUMENTO_CORDINADOR",
        tpcd.nombre as "NOMBRE_CORDINADOR",
        tpg.identificacion AS "DOCUMENTO_GERENTE",
        tpg.nombre AS "NOMBRE_GERENTE",
        tpd.identificacion AS "DOCUMENTO_DIRECTOR",
        tpd.nombre AS "NOMBRE_DIRECTOR_COMERCIAL",
        tpdc.direccion_comercial as "DIRECCION_COMERCIAL",
        tpgjc.gerencia_jefatura as "GERENCIA_COMERCIAL/ O JEFATURA",
        ter.descripcion_estado as "ESTADO",
        tdcpc.municipio  as "CIUDAD"
          
        FROM 
            fuentes_cruda.tb_datos_crudos_planta_comercial tdcpc
        INNER JOIN 
            fuentes_cruda.tb_planta_fuente tpf ON tdcpc.id_fuente = tpf.id_fuente
        INNER JOIN 
            fuentes_cruda.tb_planta_gerente tpg ON tpg.id_gerente = tdcpc.id_gerente
        INNER JOIN 
            fuentes_cruda.tb_planta_jefe tpj ON tpj.id_jefe = tdcpc.id_jefe
        INNER JOIN 
            fuentes_cruda.tb_planta_especialista tpe ON tpe.id_especialista = tdcpc.id_especialista
        INNER JOIN 
            fuentes_cruda.tb_planta_cargo tpc ON tpc.id_cargo = tdcpc.id_cargo
        INNER JOIN 
            fuentes_cruda.tb_planta_coordinador_directo tpcd ON tpcd.id_coordinador_directo = tdcpc.id_coordinador_directo
        INNER JOIN 
             fuentes_cruda.tb_planta_director tpd ON tdcpc.id_director = tpd.id_director
        INNER JOIN 
            control_procesamiento.tb_estados_registro ter  ON tdcpc.id_estado = ter.id_estado
        INNER JOIN 
            fuentes_cruda.tb_planta_estado tpe2 on tdcpc.id_estado = tpe2.id_estado       
        LEFT JOIN 
             fuentes_cruda.tb_planta_gerencia_jefatura_comercial tpgjc on tdcpc.id_gerencia_jefatura = tpgjc.id_gerencia_jefatura 
        INNER JOIN 
            fuentes_cruda.tb_planta_direccion_comercial tpdc on tdcpc.id_direccion_comercial = tpdc.id_direccion_comercial 
        WHERE 
            tdcpc."version" = (SELECT MAX("version") FROM fuentes_cruda.tb_datos_crudos_planta_comercial WHERE id_fuente = 2)
            and tdcpc.id_fuente in (1,2)
            AND tdcpc.id_segmento = 1
            AND tdcpc.id_estado in (5,1)
            AND tdcpc.id_estado != 6
            AND tpc.cargo_actual IN (
                'CONSULTOR (A) CORPORATIVO SR',
                'CONSULTOR CUENTAS CORPORATIVAS',
                'CONSULTOR(A) CORPORATIVO SR',
                'CONSULTOR(A) CUENTAS CORPORATIVAS REGIONAL SE',
                'CONSULTOR(A) CORPORATIVO REGIONAL JR',
                'CONSULTOR(A) CUENTAS CORPORATIVAS REGIONAL',
                'CONSULTOR(A) CUENTAS CORPORATIVAS',
                'CONSULTOR(A) REGIONAL INTERMEDIAS',
                'CONSULTOR(A) CORPORATIVO JR',
                'CONSULTOR IT',
                'CONSULTOR COMERCIAL JR',
                'CONSULTOR(A) CUENTAS CORPORATIVAS SR',
                'CONSULTOR(A) INTERMEDIAS SEGMENTO GE'
            );
        """

        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        # Crear DataFrame a partir de los resultados
        df = pd.DataFrame.from_records(rows, columns=[desc[0] for desc in cursor.description])

        cursor.close()
        conn.close()
        return df

    except Exception as e:
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(ejecutarConsultaInteligencia.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()  
ejecutarConsultaInteligencia()

# %%


# %%
def cruceDosDf(df1, df2, llave1): 
    """
    Realiza la concatenación de dos DataFrames y agrupa por las claves especificadas.
    Rellena valores faltantes usando ffill y bfill.
    Incluye manejo de errores para garantizar robustez.

    Parameters:
    df1 : DataFrame
        Primer DataFrame a concatenar.
    df2 : DataFrame
        Segundo DataFrame a concatenar.
    llave1 : str
        Columna por la cual agrupar los datos.

    Returns:
    DataFrame
        DataFrame resultante de la concatenación y corrección de valores faltantes.
    """
    try:
        # Concatenar los DataFrames
        concatenado = pd.concat([df1, df2], ignore_index=True)
        concatenado = concatenado.drop_duplicates()
        
        # Aplicar la interpolación de valores faltantes por cada grupo de 'llave1'
        df = concatenado.groupby(llave1, as_index=False).apply(
            lambda group: group.ffill().bfill()).reset_index(drop=True)

        df = df.drop_duplicates()
        return df
    

    except Exception as e:
        print(f'Error: {e}')
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(cruceDosDf.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


    

# %%


# %%
def crearHfc():
    """
    Realiza la carga, limpieza, renombrado y concatenación de dos DataFrames, 
    y filtra las columnas relevantes para un análisis posterior.
    También maneja errores de manera robusta.
    
    Argumentos:
        None
    
    Retorna:
        DataFrame: Contiene los datos procesados y filtrados.
    
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    try:
    
        ruta_archivos = par.ruta_archivos+par.archivo_hfc
        inst = pd.read_excel(ruta_archivos, sheet_name='inst')
        adicionales = pd.read_excel(ruta_archivos, sheet_name='adicionales')

        inst = inst[inst['PrimeroDeSEGMENTO'] == 'EMPRESAS']
        adicionales = adicionales[adicionales['PrimeroDeSEGMENTO'] == 'EMPRESAS']

        inst['CVACCO'] = 20

        # Concatenar las columnas de fecha en ambos DataFrames
        inst['FECHA'] = '20' + inst['CVAYCO'].astype(str) + '-' + inst['CVAMCO'].astype(str) + '-' + inst['CVADCO'].astype(str)
        adicionales['FECHA'] = '20' + adicionales['CVAYCO'].astype(str) + '-' + adicionales['CVAMCO'].astype(str) + '-' + adicionales['CVADCO'].astype(str)
        
        inst['CVAYCO'] = '20' + inst['CVAYCO'].astype(str)
        adicionales['CVAYCO'] = '20' + adicionales['CVAYCO'].astype(str)

        # Renombrar las columnas en 'inst'
        inst.rename(columns={
            'D_Distrito': 'CIUDAD',
            'CVAYCO': 'AÑO REPORTE',
            'CVAMCO': 'MES',
            'CVALNM': 'RAZON_SOCIAL',
            'CVACCT': 'ID',
            'CVAOÑ': 'OT',
            'CVANCT': 'NO CONTRATO',
            'D-Tipo': 'RED',
            'PRODGENERAL': 'SERVICIO_ONYX',
            'PROD VENTA DIARIO': 'CLASE',
            'CVAASE': 'NOMBRE CONSULTOR',
            'CVADLR2': 'DOCUMENTO_CONSULTOR',
            'CVARME': 'TOTAL_CIFRA_DE_ALTAS_(Total mensualidad)'
        }, inplace=True)

        # Renombrar las columnas en 'adicionales'
        adicionales.rename(columns={
            'CVAYCO': 'AÑO REPORTE',
            'CVACCO': 'DIA',
            'CVAMCO': 'MES',
            'CVALNM': 'RAZON_SOCIAL',
            'CVACCT': 'ID',
            'CVAOÑ': 'OT',
            'CVANCT': 'NO CONTRATO',
            'D-Tipo2': 'RED',
            'PRODGENERAL': 'SERVICIO_ONYX',
            'PROD VENTA DIARIO': 'CLASE',
            'CVDSCT': 'ESTADO_ORDEN_TRABAJO',
            'CVAASE': 'NOMBRE CONSULTOR',
            'CVADLR2': 'DOCUMENTO_CONSULTOR',
            'CVARME': 'TOTAL_CIFRA_DE_ALTAS_(Total mensualidad)'
        }, inplace=True)

        inst['DOCUMENTO_CONSULTOR'] = inst['DOCUMENTO_CONSULTOR'].astype('int')
        adicionales['DOCUMENTO_CONSULTOR'] = adicionales['DOCUMENTO_CONSULTOR'].astype('int')

        inst['NOMBRE_CONSULTOR_ALTA'] = inst['NOMBRE CONSULTOR']
        adicionales['NOMBRE_CONSULTOR_ALTA'] = adicionales['NOMBRE CONSULTOR']

        # Asegurarse de que ambos DataFrames tengan las mismas columnas antes de concatenar
        comunes_columnas = list(set(inst.columns).union(set(adicionales.columns)))
        inst = inst.reindex(columns=comunes_columnas)
        adicionales = adicionales.reindex(columns=comunes_columnas)

        # Concatenar los DataFrames 'inst' y 'adicionales'
        hfc = pd.concat([inst, adicionales], ignore_index=True)
        hfc['FUENTE'] = 'HFC'

        # Definir las columnas que se desean conservar
        columnas_renombradas = [
            'CIUDAD', 'AÑO REPORTE', 'MES', 'RAZON_SOCIAL', 'ID', 'OT', 'NO CONTRATO',
            'RED', 'SERVICIO_ONYX', 'CLASE', 'NOMBRE CONSULTOR', 'DOCUMENTO_CONSULTOR', 
            'ESTADO_ORDEN_TRABAJO', 'FECHA', 'NOMBRE_CONSULTOR_ALTA', 'FUENTE', 'TOTAL_CIFRA_DE_ALTAS_(Total mensualidad)'
        ]

        # Eliminar las columnas que no están en la lista de columnas renombradas
        columnas_a_eliminar = [col for col in hfc.columns if col not in columnas_renombradas]
        hfc.drop(columns=columnas_a_eliminar, inplace=True)
        #hfc.to_excel("resultado_consulta_HFC.xlsx", index=False)


        return hfc

    except Exception as e:
            fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
            cantidad_registros.append(0)
            estado_error.append(2)
            funcion_error.append(crearHfc.__name__)
            descripcion_error.append(str(e)[:100])
            insertarErroresDB()
            salidaLogMonitoreo() 
crearHfc()                          

# %%
#sql funcion que retorne el dataframe 

# %%
def ejecutarConsultaMacrosFO():
    """
    Función encargada de ejecutar una consulta SQL en la base de datos, extraer datos relevantes
    sobre Macros F1 y F 2", y retornarlos en un DataFrame.
    
    Argumentos:
        None
    
    Retorna:
        DataFrame: Contiene los resultados de la consulta.
    
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    
    fecha_actual = datetime.now()
    #fecha_final = fecha_actual.strftime("%Y-%m-%d")
    fecha_final = fecha_actual - timedelta(days=1)
    fecha_inicial = fecha_final.replace(year=2020, month=1, day=1)
    #fecha_inicial = datetime(2020, 1, 1).strftime("%Y-%m-%d")


    conn = conexion_BD()

    
    
    query = """
SELECT 
    tdcfo.fecha_creacion AS "FECHA DE ESTADO DE LA OT",
    tdcfo.otc_diferida  as "Total_OTC_Diferida",
    tdcfo.fecha_inicio_procesamiento AS "FECHA_INICIO_PROCESAMIENTO",
    tdcfo.fecha_estado AS "FECHA",
    tdcfo.mes_estado AS "MES",
    tdcfo.nit AS "NIT_CLIENTE",
    tdcfo.id AS "ID",
    tdcfo.id_orden_trabajo_padre AS "OT",
    tdcfo.razon_social AS "RAZON_SOCIAL",
    tdcfo.id_cliente AS "ID_CLIENTE",
    tdcfo.segmento AS "SEGMENTO",
    tdcfo.segmento_mercado AS "SEGMENTO_MERCADO",
    tdcfo.nro_contrato AS "NO_CONTRATO",
    tdcfo.id_enlace AS "ENLACE",
    tdcfo.proceso_tipo_venta,
    tdcfo.tipo_venta AS "TIPO_VENTA",
    tdcfo.producto AS "SERVICIO_ONYX",
    tdcfo.linea AS "FAMILIA",
    tdcfo.num_servicios AS "CONTEO_DE_SERVICIOS",
    tdcfo."velocidad" AS "VELOCIDAD",
    tdcfo.ciudad_origen AS "CIUDAD_ORIGEN_VENTA",
    tdcfo.ciudad_destino AS "CIUDAD_DESTINO_VENTA",
    tdcfo.trm_creacion AS "TRM",
    tdcfo.duracion_contrato AS "DURACION_CONTRATO",
    tdcfo.monto_moneda_local_otros AS "CARGO_INSTALACION (SISTEMA)",
    tdcfo.monto_moneda_local_cargo_mensual as "CARGO_MENSUAL_SISTEMA",
    tdcfo.descripcion AS "OBSERVACIONES",
    tdcfo.nombre_consultor AS "NOMBRE_CONSULTOR",
    tdcfo.cedula_comercial AS "DOCUMENTO_CONSULTOR",
    eot.estado_orden_trabajo AS "ESTADO_ORDEN_TRABAJO", 
    'MACROS' as FUENTE
FROM (
    SELECT 
        tdcfo.*,
        ROW_NUMBER() OVER (
            PARTITION BY tdcfo.id_orden_trabajo_padre 
            ORDER BY 
                CASE 
                    WHEN tdcfo.ciudad_origen like 'SIN CIUDA%' THEN 2 
                    ELSE 1 
                END, 
                tdcfo.fecha_inicio_procesamiento DESC 
        ) AS rn
    FROM "DBInteligenciaComercial".fuentes_cruda.tb_datos_crudos_fibra_optica tdcfo
    WHERE tdcfo.id_estado_orden_trabajo != 13
) tdcfo
INNER JOIN "DBInteligenciaComercial".fuentes_cruda.tb_fo_estado_orden_trabajo AS EOT  
    ON tdcfo.id_estado_orden_trabajo = eot.id_fo_estado
WHERE tdcfo.rn = 1
and  tdcfo.id_orden_trabajo_padre <> '0'
AND tdcfo.id_estado_venta = 3
AND tdcfo.direccion_comercial IN ('DIR. EMPRESAS GRANDES','DIR. EMPRESAS ESTRATEGICAS')
AND id_fo_estado = 4;
        """
        
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        

        df = pd.DataFrame.from_records(rows, columns=[desc[0] for desc in cursor.description])
        
        cursor.close()
        conn.close()
        # Exportar a Excel
        #nombre_archivo = f"MacrosFO_{fecha_actual.strftime('%Y%m%d')}.xlsx"
        #df.to_excel(nombre_archivo, index=False)
        #print(f"Archivo exportado exitosamente: {nombre_archivo}")

        return df

    except Exception as e:
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(ejecutarConsultaMacrosFO.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()  
ejecutarConsultaMacrosFO()                              

# %%


# %% [markdown]
# 

# %%
def ejecutarConsultaAsignacion():
    """
     Método para ejecutar una consulta y devolver los resultados en un DataFrame.
    
    Este método se conecta a la base de datos de producción, ejecuta una consulta SQL que obtiene información 
    relacionada con los segmentos de empresas y la asignación de consultores, gerentes y jefes, y devuelve los 
    resultados en un DataFrame de Pandas.
    
    Argumentos:
        None
    
    Retorna:
        DataFrame: Contiene los resultados de la consulta.
    
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    conn = conexion_BD_produccion() 
    
    query = """
    select 
    bse."NIT" ,
    bse."SEGMENTO CLIENTE",
    bse."NIT DV",
    bse."RAZON SOCIAL",
    bse."SEGMENTO ACTUAL",
    bse."SECTOR" as "SECTOR ECONOMICO",
    BSE."ACTIVIDAD COMERCIAL",
    bse."CIUDAD" as "CIUDAD CLIENTE",
    BSE."CONVERGENTE EN ASIGNACION" ,
    bse."GRUPO OBJETIVO",
    bse."DEPARTAMENTO",
    bse."CONSULTOR ACTUAL FIJO",
    bse."CEDULA CONSULTOR FIJO",
    bse."CONSULTOR ANTERIOR FIJO 1",
    bse."CONSULTOR ACTUAL MOVIL",
    bse."CEDULA CONSULTOR MOVIL",
    BSE."CONSULTOR ANTERIOR MOVIL 1", 
    bse."GERENTE ACTUAL MOVIL",
    bse."GERENTE ACTUAL FIJO",
    bse."JEFE ACTUAL FIJO",
    bse."JEFE ACTUAL MOVIL",
    bse."TOTAL RECURRENTE FO Y HFC",
    bse."TOTAL RECURRENTE FO",
    bse."TOTAL RECURRENTE HFC" 
    from inteligencia_comercial.public.base_segmento_empresas bse 
    where "SEGMENTO CLIENTE" = 'EMPRESAS'
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        df = pd.DataFrame.from_records(rows, columns=[desc[0] for desc in cursor.description])
        cursor.close()
        conn.close()
        
        return df
    
    except Exception as e:
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(ejecutarConsultaAsignacion.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()              

# %%
def crearCloud():
    """
    Función para crear un DataFrame a partir de un archivo de Excel con información de ventas Cloud.

    Argumentos:
        None

    Retorna:
        DataFrame: Contiene la información de ventas Cloud.
    """
    try:
        cloud = pd.read_excel(
         r"C:\Users\perezjomi\Comunicacion Celular S.A.- Comcel S.A\Gerencia Inteligencia Comercial - desarrollo altas 2024\Archivos entrada Altas\Reporte de Ventas Cloud mes_20022025.xlsx", 
        sheet_name='base ventas Cloud')


        #ruta = par.ruta_cloud + par.archivo_cloud
        #cloud = pd.read_excel(ruta, sheet_name='base ventas Cloud')

        # Definir las columnas requeridas
        columnas_requeridas = [
            'CEDULA COMERCIAL', 'NOMBRES', 'CARGO', 'JEFATURA', 'GERENCIA', 'DIRECTOR', 'AÑO',
            'FECHA DE VENTA', 'MES', 'ID CLIENTE', 'NIT', 'RAZON SOCIAL', 'SEGMENTO', 'ORDEN',
            'SEGMENTO DE ALTA', 'RED', 'TIPO DE VENTA', 'PRODUCTO', 'TIPOSERVICIO', 'No SERVICIOS',
            'VALOR UNITARIO SIN IVA', 'OPERACION','ESTADO COMERCIAL INICIAL DEL SERVICIO VENDIDO',
            'ESTADO COMERCIAL FINAL DEL SERVICIO VENDIDO'
        ]


        # Filtrar las columnas requeridas
        cloud = cloud[columnas_requeridas]

        cloud.rename(columns={
            'CEDULA COMERCIAL': 'DOCUMENTO_CONSULTOR',
            'NOMBRES': 'NOMBRE CONSULTOR',
            'CARGO': 'CARGO_ACTUAL',
            'JEFATURA': 'NOMBRE_JEFE',
            'GERENCIA': 'NOMBRE_GERENTE',
            'DIRECTOR': 'NOMBRE_DIRECTOR_COMERCIAL',
            'AÑO': 'AÑO REPORTE',
            'FECHA DE VENTA': 'FECHA',
            'MES': 'MES',
            'ID CLIENTE': 'ID_CLIENTE',
            'NIT': 'NIT CLIENTE',
            'RAZON SOCIAL': 'RAZON SOCIAL',
            'SEGMENTO': 'SEGMENTO',
            'ORDEN': 'OT',
            'RED': 'RED',
            'TIPO DE VENTA': 'TIPO_VENTA',
            'PRODUCTO': 'SERVICIO_ONYX',
            'TIPOSERVICIO': 'RETO',
            'No SERVICIOS': 'CONTEO_DE_SERVICIOS',
            'VALOR UNITARIO SIN IVA': 'TOTAL_CIFRA_DE_ALTAS_(Total mensualidad)'
        }, inplace=True)

        cloud['FUENTE'] = 'CLOUD'
        cloud = cloud[cloud['OPERACION'] == 'FIJO']
        cloud = cloud[cloud['SEGMENTO DE ALTA'] == 'EMPRESAS']

        cloud = cloud[
        cloud['ESTADO COMERCIAL INICIAL DEL SERVICIO VENDIDO'].str.contains("Activo", case=False, na=False) |
        cloud['ESTADO COMERCIAL FINAL DEL SERVICIO VENDIDO'].str.contains("Activo", case=False, na=False)]
        
        return cloud

    except Exception as e:
        # Manejo de errores en caso de fallos
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven,par.cl]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(crearCloud.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()          

# %%


# %%
def cruceConsultaAsignacion(new_up, hfc_int):
    """
    Realiza un cruce entre datos de archivos y asignaciones, ajustando el formato y completando columnas faltantes.
    Devuelve un DataFrame ordenado según un conjunto predefinido de columnas.
    
    Parámetros:
        new_up (DataFrame): DataFrame con los datos actualizados de clientes.
        vent_hfc_int (DataFrame): DataFrame con información de ventas HFC.
    
    Retorna:
        DataFrame: Resultado del cruce con la asignación de consultores, gerentes y jefes.
    
    Manejo de errores:
        En caso de excepción, registra el error en la base de datos y genera un log de monitoreo.
    """
    try:
        archivos = cruceDosDf(new_up, hfc_int, 'OT')
    
        asignacion = ejecutarConsultaAsignacion()

        asignacion = asignacion.assign(
            NOMBRE_CONSULTOR=asignacion['CONSULTOR ACTUAL FIJO'].combine_first(asignacion['CONSULTOR ACTUAL MOVIL']),
            NOMBRE_GERENTE=asignacion['GERENTE ACTUAL FIJO'].combine_first(asignacion['GERENTE ACTUAL MOVIL']),
            NOMBRE_JEFE=asignacion['JEFE ACTUAL FIJO'].combine_first(asignacion['JEFE ACTUAL MOVIL']),
            DOCUMENTO_CONSULTOR=asignacion['CEDULA CONSULTOR FIJO'].combine_first(asignacion['CEDULA CONSULTOR MOVIL'])
        )
        
        asignacion = asignacion.rename(columns={'NOMBRE_CONSULTOR': 'NOMBRE CONSULTOR'})
      
        cruce_nits = pd.merge(archivos, asignacion, how='left', left_on=['NIT_CLIENTE', 'RAZON_SOCIAL'], right_on=['NIT DV', 'RAZON SOCIAL'])

        cruce_nits['NIT CLIENTE'] = cruce_nits['NIT_CLIENTE'].combine_first(cruce_nits['NIT DV'])
        
        columnas_combinadas = {
            col.replace('_x', ''): cruce_nits[col].combine_first(cruce_nits[col.replace('_x', '_y')])
            for col in cruce_nits.columns if col.endswith('_x')
        }
        cruce_nits = cruce_nits.assign(**columnas_combinadas)

        columnas_a_eliminar = [
            col for col in cruce_nits.columns if col.endswith('_x') or col.endswith('_y')
        ] + ['NIT_CLIENTE', 'NIT DV', 'CONSULTOR ACTUAL MOVIL', 'GERENTE ACTUAL MOVIL', 
            'JEFE ACTUAL MOVIL', 'CEDULA CONSULTOR MOVIL']
        resultado_final = cruce_nits.drop(columns=columnas_a_eliminar)

        resultado_final = resultado_final.drop_duplicates()
        
        return resultado_final

    except Exception as e:
            #fuentes.append(par.nov+" | "+par.can+" | "+par.planta+" | "+par.asig+" | "+par.hfc+" | "+par.ven+" | "+par.fo)
            fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
            cantidad_registros.append(0)
            estado_error.append(2)
            funcion_error.append(ejecutarConsultaAsignacion.__name__)
            descripcion_error.append(str(e)[:100])
            insertarErroresDB()
            salidaLogMonitoreo()

# %%


# %%

# Variables globales para mantener el estado de mes y consecutivo
estado = {"mes_actual": None, "consecutivo": 0}

def generarConsecutivo():
    """
    Genera un consecutivo único basado en el mes actual y un contador interno.
    Incluye manejo de errores para situaciones inesperadas.
    """
    global estado 

    MESES = {
        1: "ENE", 2: "FEB", 3: "MAR", 4: "ABR", 5: "MAY", 6: "JUN",
        7: "JUL", 8: "AGO", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DIC"
    }

    try:
        
        hoy = datetime.now()

        # Validar el mes actual para evitar errores en el diccionario
        mes_abreviado = MESES.get(hoy.month)
        if not mes_abreviado:
            raise ValueError(f"Mes {hoy.month} no válido en el diccionario de MESES.")

        anio = hoy.strftime("%y") 

        # Reinicia el consecutivo si el mes cambia
        if estado["mes_actual"] != mes_abreviado:
            estado["mes_actual"] = mes_abreviado
            estado["consecutivo"] = 0  

        estado["consecutivo"] += 1

        # Generar el consecutivo en el formato solicitado
        consecutivo_formateado = f"ALT-{mes_abreviado}{anio}-{estado['consecutivo']:08d}"
        return consecutivo_formateado
    
    except Exception as e:
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(generarConsecutivo.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()              

# %%
def procesarDataFrame(resultado_final, columnas_nuevas):
    """
     Procesa el DataFrame generando consecutivos y reordenando las columnas.
    Maneja errores registrándolos en la base de datos y en el log.

    Parámetros:
    - resultado_final (DataFrame): El DataFrame a procesar.
    - columnas_nuevas (list): Lista de columnas en el orden deseado.

    Retorna:
    - DataFrame: El DataFrame procesado y reordenado.
    - None: En caso de que ocurra un error.
    
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
   """
    try:
        print(len(resultado_final))
        resultado_final['NUV'] = [generarConsecutivo() for _ in range(len(resultado_final))]
        print(len(resultado_final))

        columnas_orden = ['NUV'] + [col for col in columnas_nuevas if col != 'NUV']
        for columna in columnas_orden:
            if columna not in resultado_final.columns:
                resultado_final[columna] = np.nan 

        resultado_ordenado = resultado_final[columnas_orden]
        del resultado_final  
        return resultado_ordenado

    except Exception as e:
        print(e)
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.apped(2)
        funcion_error.append(procesarDataFrame.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()         

# %%

 
def transformarColumnas(df, columnas):
    """Transforma las columnas especificadas del DataFrame.
 
    Convierte las columnas a numérico, maneja errores, divide por 1000.
 
    Args:
        df: El DataFrame de pandas.
        columnas: Una lista de nombres de columnas para transformar.

    Returns:
        El DataFrame con las columnas transformadas.
 
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    try:
    
        df[columnas] = df[columnas].apply(pd.to_numeric, errors='coerce').fillna(0)
        df[columnas] = df[columnas] / 1000
 
        return df
 
    except Exception as e:
        print(e)
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(transformarColumnas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
 
def formatearMoneda(valor):
    """Formatea un número como moneda sin cambiar su tipo."""
    return f"${valor:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")

# %%
def consolidarInformacion():
    """
    
Función encargada de consolidar información de diversas fuentes, realizar cruces de datos
    y generar un DataFrame final con la información consolidada.
    
    Argumentos:
        None
    
    Retorna:
        DataFrame: Contiene la información consolidada.
    
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente

    """
    try:

        df_new = ejecutarConsultaOdbcSltasNew()
        df_upselling = ejecutarConsultaOdbcUpselling()
        df_crucedos = cruceDosDf(df_new, df_upselling, 'OT')
        df_crucedos = df_crucedos.drop_duplicates()
        df_hfc = crearHfc()
        df_plantacomercial = ejecutarConsultaInteligencia()
        
        
        # Filtrar el DataFrame df_plantacomercial
        inteligencia_sin_it = df_plantacomercial[
            ~((df_plantacomercial['CARGO_ACTUAL'] == 'CONSULTOR IT') & 
              (df_plantacomercial['NOMBRE CONSULTOR'] != 'ELSA PATRICIA AVELLANEDA SUAREZ'))
        ]

        # Realizar la unión entre df_hfc e inteligencia_sin_it
        df_union = pd.merge(df_hfc, inteligencia_sin_it, how='left', on='DOCUMENTO_CONSULTOR')
       
        

        # Se modifica los x y y para validar si tenemos primero la información de planta
        df_union['CIUDAD'] = df_union['CIUDAD_x'].combine_first(df_union['CIUDAD_y'])
        df_union['NOMBRE CONSULTOR'] = df_union['NOMBRE CONSULTOR_y'].combine_first(df_union['NOMBRE CONSULTOR_x'])
        df_union = df_union.drop(columns=['CIUDAD_x', 'CIUDAD_y', 'NOMBRE CONSULTOR_x', 'NOMBRE CONSULTOR_y'])
        df_union = df_union.drop_duplicates()
        

        df_macro = ejecutarConsultaMacrosFO()
        df_macro.columns = df_macro.columns.str.upper()
    
        total_macro = cruceDosDf(df_union , df_macro, 'OT')
        #merge entre df union y df macro, left join por el numero de la OT
        total_macro = total_macro.drop_duplicates()

        df_cloud = crearCloud()
        total_cloud = cruceDosDf(total_macro, df_cloud, 'OT')
        total_cloud = total_cloud.drop_duplicates()


        # Cruzar los DataFrames de asignación
        resultado= cruceConsultaAsignacion(df_crucedos, total_cloud)

        # Cruzar los DataFrames de cloud
        
        resultado_final = cruceDosDf(resultado, df_cloud, 'OT')
        resultado_final = resultado_final.drop_duplicates()
        
        
        inteligencia_it = df_plantacomercial[
            ((df_plantacomercial['CARGO_ACTUAL'] == 'CONSULTOR IT') & 
             (df_plantacomercial['NOMBRE CONSULTOR'] != 'ELSA PATRICIA AVELLANEDA SUAREZ'))
        ]
        inteligencia_it = inteligencia_it[
            ['NOMBRE CONSULTOR', 'DOCUMENTO_CONSULTOR', 'DOCUMENTO_CORDINADOR', 'NOMBRE_CORDINADOR']
        ]
        inteligencia_it.rename(
            columns={
                'NOMBRE CONSULTOR': 'NOMBRE_CONSULTOR IT',
                'DOCUMENTO_CONSULTOR': 'DOCUMENTO_CONSULTOR IT',
                'DOCUMENTO_CORDINADOR': 'DOCUMENTO_COORDINADOR IT',
                'NOMBRE_CORDINADOR': 'COORDINADOR IT'
            }, inplace=True
        )
       
        resultado_final['DOCUMENTO_CONSULTOR'] = pd.to_numeric(resultado_final['DOCUMENTO_CONSULTOR'], errors='coerce')
        resultado_final = pd.merge(resultado_final, inteligencia_sin_it, how='left', on='DOCUMENTO_CONSULTOR')

        columnas_x = [col for col in resultado_final.columns if col.endswith('_x')]
        columnas_y = [col.replace('_x', '_y') for col in columnas_x]
        for col_x, col_y in zip(columnas_x, columnas_y):
            resultado_final[col_x.replace('_x', '')] = resultado_final[col_x].combine_first(resultado_final[col_y])
        resultado_final = resultado_final.drop(columns=columnas_x + columnas_y)


        resultado_final['NOMBRE_JEFE'] = resultado_final['NOMBRE_JEFE'].replace(['NO APLICA', '', None], 'Sin Asignar')

        resultado_final['AÑO REPORTE'] = fecha_actual.strftime('%Y')
        resultado_final['FECHA'] = pd.to_datetime(resultado_final['FECHA'], errors='coerce')
        resultado_final['FECHA'] = resultado_final['FECHA'].dt.strftime(r'%Y-%m-%d')

        # Limpieza de espacios en columnas de tipo string
        resultado_final = resultado_final.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Reemplazar los valores 1234 por 0 en las columnas especificadas
        columnas_a_modificar = ['DOCUMENTO_CONSULTOR', 'DOCUMENTO_JEFE', 'DOCUMENTO_GERENTE', 'DOCUMENTO_DIRECTOR']
        resultado_final[columnas_a_modificar] = resultado_final[columnas_a_modificar].replace(1234, 0)
        if 'ESTADO_CONSULTOR' in resultado_final.columns:
            resultado_final[columnas_a_modificar] = resultado_final[columnas_a_modificar].replace('', 0).fillna(0)

        if 'ESTADO_CONSULTOR' in resultado_final.columns:
            resultado_final['ESTADO_CONSULTOR'] = resultado_final.apply(
                lambda row: "INACTIVO" if (isinstance(row['CONSULTOR ACTUAL FIJO'], str) and "(VACANTE)" in row['CONSULTOR ACTUAL FIJO']) or row['CEDULA CONSULTOR FIJO'] == 'None'
                else row['ESTADO_CONSULTOR'], axis=1
            )
            resultado_final['ESTADO_CONSULTOR'] = resultado_final['ESTADO_CONSULTOR'].replace('').fillna('PENDIENTE_ESTADO')

        if 'CEDULA CONSULTOR FIJO' in resultado_final.columns:
            resultado_final['CEDULA CONSULTOR FIJO'] = resultado_final['CEDULA CONSULTOR FIJO'].replace(['None', ''], 0).fillna(0)

    
        
        resultado_final['NOMBRE_CONSULTOR_ALTA'] = resultado_final['NOMBRE_CONSULTOR_ALTA'].fillna(resultado_final['NOMBRE CONSULTOR'])
        resultado_final['NOMBRE CONSULTOR'] = resultado_final['NOMBRE CONSULTOR'].replace('', 'SIN ASIGNACION').fillna('SIN ASIGNACION')
       
        resultado_final['TOTAL_SIAO'] = resultado_final['TOTAL_CIFRA_DE_ALTAS_(Total mensualidad)']


        'OT REMPLAZADA Y/O DESGLOZADA', 'Total_OTC_Diferida'
        columnas = [
            'TRM', 'CARGO_MENSUAL_SISTEMA', 'CARGO_INSTALACION (SISTEMA)',
            'TOTAL_SIAO', 'TOTAL_CIFRA_DE_ALTAS_(Total mensualidad)'
        ]

        resultado_final = transformarColumnas(resultado_final, columnas)
        resultado_final[columnas] = resultado_final[columnas].astype(float).round(2)


        # Resetear el índice
        resultado_final.reset_index(drop=True, inplace=True)
        resultado_final = resultado_final.drop_duplicates()

    
        return resultado_final

    except Exception as e:
        print(e)
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append("consolidar_informacion.__name__")
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def formatearExcel(writer, dataframe):
    """
    Aplica formatos a la hoja de Excel.

    Argumentos:
        writer (pd.ExcelWriter): Escritor de Excel donde se aplicarán los formatos.
        dataframe (pd.DataFrame): DataFrame con los datos exportados.

    Excepciones manejadas:
        Exception: Captura el error si ocurre un problema al aplicar formatos.
    """
    try:
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        
        # Formato de moneda
        formato_moneda = workbook.add_format({'num_format': '"$"#,##0.00'})

        # Formato para los encabezados
        formato_encabezado = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#ADD8E6', 
            'border': 1
        })

        # Aplicar formato a los encabezados
        for col_num, value in enumerate(dataframe.columns.values):
            worksheet.write(0, col_num, value, formato_encabezado)
            worksheet.set_column(col_num, col_num, len(value) + 2)  # Ajustar ancho de columna

        # Aplicar formato a columnas específicas
        columnas_formateo = ['BE', 'BH', 'BI', 'BL', 'BQ', 'BZ', 'CA', 'BY']
        for columna in columnas_formateo:
            worksheet.set_column(f'{columna}:{columna}', None, formato_moneda)

    except Exception as e:
        print(f"Error al aplicar formatos a la hoja de Excel: {e}")
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(formatearExcel.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%

def exportarExcel(dataframe, nombre_archivo):
    """
    Exporta un DataFrame a un archivo Excel y aplica formatos.

    Argumentos:
        dataframe (pd.DataFrame): DataFrame a exportar.
        nombre_archivo (str): Nombre del archivo de salida.
    
    Excepciones manejadas:
        Exception: Captura el error en caso de que falle la exportación.
    """
    try:
        with pd.ExcelWriter(nombre_archivo, engine='xlsxwriter') as writer:
            dataframe.to_excel(writer, sheet_name='Sheet1', index=False)
            formatearExcel(writer, dataframe)  # Aplicar formato
        print(f"Archivo exportado exitosamente como {nombre_archivo}")
    except Exception as e:
        print(f"Error al exportar a Excel: {e}")
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append(exportarExcel.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()
              

# %%
if __name__ == "__main__":

    """
    Bloque principal de ejecución del script.

    Este bloque se encarga de:
    - Generar un ID de ejecución único.
    - Registrar la fecha y hora de inicio del proceso.
    - Consolidar información desde diferentes fuentes.
    - Procesar el DataFrame resultante y exportarlo a un archivo Excel.
    - Registrar la duración del proceso y manejar cualquier excepción que ocurra.

    Argumentos:
        None

    Retorna:
        None

    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    try:
        id_ejecucion = generate_uuid().upper()
        fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")

        # Consolidar información desde diferentes fuentes
        resultado = consolidarInformacion()
        estado = {"mes_actual": None, "consecutivo": 0}

        columnas_nuevas = [
            "NUV", "INSERTADO", "CIUDAD", "DOCUMENTO_CONSULTOR", "NOMBRE CONSULTOR",
            "ESTADO_CONSULTOR", "CONSULTOR ACTUAL FIJO", "CEDULA CONSULTOR FIJO", "CARGO_ACTUAL",
            "CONSULTOR ANTERIOR FIJO 1", "DOCUMENTO_JEFE", "NOMBRE_JEFE", "DOCUMENTO_GERENTE",
            "NOMBRE_GERENTE", "DOCUMENTO_DIRECTOR", "NOMBRE_DIRECTOR_COMERCIAL", "DIRECCION_COMERCIAL",
            "GERENCIA_COMERCIAL/ O JEFATURA", "DOCUMENTO_CONSULTOR IT", "CONSULTOR IT",
            "DOCUMENTO_COORDINADOR IT", "COORDINADOR IT", "AÑO REPORTE", "FECHA", "MES", "ID_CLIENTE",
            "NIT", "NIT CLIENTE", "RAZON SOCIAL", "CIUDAD CLIENTE", "SEGMENTO", "DIVISION",
            "SECTOR ECONOMICO", "ID", "OT", "FUENTE", "NO_CONTRATO", "ENLACE", "RED", "TIPO_VENTA",
            "SERVICIO_ONYX", "FAMILIA", "CLASE", "COMPONENTE", "RETO", "UNIDAD DE NEGOCIO",
            "FAMILIA_AMX", "CONTEO_DE_SERVICIOS", "NO LINEAS", "NUMERO DE EXTENSIONES PBX",
            "VELOCIDAD", "CLASE VENTA", "CIUDAD_ORIGEN_VENTA", "DEPARTAMENTO ORIGEN VENTA",
            "DEPARTAMENTO DESTINO VENTA", "CIUDAD_DESTINO_VENTA", "TRM", "MONEDA",
            "DURACION_CONTRATO", "CARGO_INSTALACION (SISTEMA)", "CARGO_MENSUAL_SISTEMA",
            "ALQUILER_EQUIPOS_(SISTEMA)", "DEFINICION", "OT REMPLAZADA Y/O DESGLOZADA",
            "CARGO INSTALACION CIFRAS OFICIALES", "CARGO MENSUAL_CIFRAS_OFICIALES",
            "ALQUILER_EQUIPOS_CIFRAS_OFICIALES", "TOTAL_MENSUALIDAD", "TOTAL CONTRATO",
            "OBSERVACIONES", "SERVICIOS_EMPAQUETADOS_HITTS", "PROYECTO ESPECIAL",
            "CODIGO SALEFORCES", "ESTADO CONTRATO", "ACUERDOS DE INDICADOR",
            "TOTAL_PAGO_A_COMISIONES", "TOTAL_CIFRA_DE_ALTAS_(Total mensualidad)",
            "Total_OTC_Diferida", "TOTAL_SIAO", "ESTADO_ORDEN_TRABAJO",
            "FECHA DE ESTADO DE LA OT", "NOMBRE_CONSULTOR_ALTA"
        ]

        resultado_final = procesarDataFrame(resultado, columnas_nuevas)
        fecha_hoy = datetime.now().strftime("%d-%m-%Y")
        nombre_archivo = f"Informe_Consolidado_Altas_{fecha_hoy}.xlsx"
        exportarExcel(resultado_final, nombre_archivo)

        if resultado_final is not None:
            registros = len(resultado_final)
            fuentes = [" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven])]

        
            df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, fuentes, registros,  par.destino_altas, 1)

        fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
        duracion_proceso = fecha_fin_date - fecha_inicio_date
        duracion_proceso_seg = int(duracion_proceso.total_seconds())
        actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date, duracion_proceso_seg)

        duracion.append(str(duracion_proceso))
        estado_error.append(1)
        salidaLogMonitoreo()

    except Exception as e:
        print(f"Error: {e}")
        fuentes.append(" | ".join([par.nov, par.can, par.planta, par.asig, par.fo, par.hfc, par.ven]))
        cantidad_registros.append(0)
        estado_error.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%



# %%


# %%



