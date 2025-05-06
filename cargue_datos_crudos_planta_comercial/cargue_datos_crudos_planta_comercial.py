# %%
"""
***************************************************************************************
* CLARO  HITSS - EMPRESAS Y NEGOCIOS                                                  *
* OBJETIVO: Extración de fuentes crudas de planta comercial                           * 
*           y cargue a base de datos de forma automatica                              *
*           Comunicacion Celular S.A.- Comcel S.A\Wilmer Camargo Ochoa - Data_PCC     *
* TABLA DE INGESTA POSTGRESQL: tb_datos_crudos_planta_comercial                       *
* FECHA CREACION: 15 de Julio de 2024                                                 *
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
destino = [par.destino_planta_comercial]
estado = []
funcion_error = []
descripcion_error = []
id_ejecucion_en_curso = None

# %%
def importarPlantaComercial(ruta, nombre_archivo, hoja_calculo):
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

        if nombre_archivo == par.nombre_archivo_planta:
            base_excel = pd.read_excel(ruta + nombre_archivo, sheet_name=hoja_calculo)
        elif nombre_archivo == par.nombre_archivo_clasificador_geo:
            base_excel = pd.read_excel(ruta + nombre_archivo, sheet_name=hoja_calculo, dtype=str)

        base_excel['FUENTE'] = hoja_calculo
        
        return base_excel
    except Exception as e:
        logging.error(f"Error al importar archivo {nombre_archivo}: {e}")
        raise


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
        #fuentes.append('planta comercial')
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
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,'planta comercial',0,'tb_datos_crudos_planta_comercial',2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_planta)
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
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(generate_uuid.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    

# %%
def tablasDominio(df,fecha_inicio_date):
    
    campos_tablas_dominio = [
        'TIPO DOCUMENTO', 'GENERO', 'CARGO ACTUAL', 'OPERACION', 'CONTRATO',
        'TIPO DE CONTRATACION', 'CONTRATANTE', 'NOMBRE ESPECIALISTA', 'NOMBRE COORDINADOR TERCERO',
        'NOMBRE COORDINADOR DIRECTO', 'NOMBRE JEFE', 'DOCUMENTO JEFE', 'NOMBRE GERENTE', 'DOCUMENTO GERENTE','NOMBRE DIRECTOR COMERCIAL',
        'DIRECCION COMERCIAL', 'SEGMENTO', 'GERENCIA COMERCIAL/ O JEFATURA', 'GRUPO COMERCIAL', 'AREA',
        'CANAL', 'CATEGORIA', 'CATEGORIZACION', 'PROVEEDOR', 'CIUDAD', 'REGIONAL', 'DEPARTAMENTO',
        'CODIGO DANE', 'ANTIGUEDAD', 'ESTADO', 'ESPECIALISTA','GERENTE','ESPECILSITA','JEFE',
        'CEDULA','JEFE DIRECTO','OPERACIÓN','NOMBRE DE GERENTE','GERENCIA','NOMBRE DIRECTOR','NOMBRE'
    ]
    
    dataframes = {}
    #print(df)
    for campo in campos_tablas_dominio:
        if campo in df.columns:
            df_campo = pd.DataFrame({campo: df[campo].astype(str)
                                                        .str.upper()
                                                        .str.strip()
                                                        .str.replace('\n', '', regex=True)
                                                        .str.replace('\r', '', regex=True)
                                                        .str.replace('\t', '', regex=True)
                                                        .str.replace('  ', '', regex=True)
                                                        .unique()})
            df_campo = df_campo[df_campo[campo] != 'NO APLICA']
            df_campo = df_campo[df_campo[campo] != 'NAN']
            df_campo = df_campo[df_campo[campo].notnull()]
            df_campo['fecha_creacion'] = fecha_inicio_date
            df_campo['fecha_modificacion'] = fecha_inicio_date
            df_campo['id_estado_registro'] = 1
            
            dataframes[campo] = df_campo
    
    return dataframes

# %%
def tablasDominioRetail(df,fecha_inicio_date):
    campos_tablas_dominio = [
        'ESPECIALISTA','GERENTE','CIUDAD'
    ]
    
    dataframes = {}
    #print(df)
    for campo in campos_tablas_dominio:
        if campo in df.columns:
            df_campo = pd.DataFrame({campo: df[campo].astype(str)
                                                        .str.upper()
                                                        .str.strip()
                                                        .str.replace('\n', '', regex=True)
                                                        .str.replace('\r', '', regex=True)
                                                        .str.replace('\t', '', regex=True)
                                                        .str.replace('  ', '', regex=True)
                                                        .unique()})
            df_campo = df_campo[df_campo[campo] != 'NO APLICA']
            df_campo = df_campo[df_campo[campo] != 'NAN']
            df_campo = df_campo[df_campo[campo].notnull()]
            df_campo['fecha_creacion'] = fecha_inicio_date
            df_campo['fecha_modificacion'] = fecha_inicio_date
            df_campo['id_estado_registro'] = 1
            
            dataframes[campo] = df_campo
    
    return dataframes

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
        
        #print(nombre_tabla)
        if 'id' in df_final.columns:
            df_final['id'] = [str(uuid.uuid4()) for _ in range(len(df_final))]

        with conexion.begin() as conn:
            df_final.to_sql(nombre_tabla, con=conn, schema=nombre_esquema, if_exists='append', index=False)
        print(f"Datos insertados correctamente en {nombre_esquema}.{nombre_tabla}")
    
           
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_planta)
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
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(actualizarFechaFinProcesamiento.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()

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
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarTablasPlantaComercialHistorico.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()
    

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
        fuentes.append(par.nombre_archivo_planta)
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
def ajustar_cedula(identificacion):
    """
    Ajusta el número de cédula al formato estándar:
    - Si tiene 11 dígitos y comienza con '1': recortar a 10 dígitos
    - Si tiene 9 dígitos y comienza con '1': recortar a 8 dígitos
    
    Args:
        identificacion (str): Número de cédula a ajustar
    
    Returns:
        str: Cédula ajustada
        None: En caso de error
    """
    try:
        identificacion = str(identificacion).strip()
        
        # Caso 1: longitud 11
        if len(identificacion) == 11 and identificacion.startswith('1'):
            identificacion = identificacion[1:]  # Remover primer dígito
            
        # Caso 2: longitud 9    
        elif len(identificacion) == 9 and identificacion.startswith('1'):
            identificacion = identificacion[1:]  # Remover primer dígito

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(ajustar_cedula.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()
        
    return identificacion

# %%
def preparacionCargueTablaJefe(df_planta_comercial, origen):
    """
    Función que prepara y carga la tabla de jefes en la base de datos a partir del archivo de planta comercial.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial que contiene la información de los empleados.
        origen: Indica de qué hoja (tipo de red) provienen los datos (e.g., "retiro", "cavs", etc.).

    Retorna:
        None

    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    try:
        # Filtrar por cargo para encontrar jefes
        df_jefes = df_planta_comercial[df_planta_comercial['CARGO ACTUAL'].str.contains("Jefe", case=False, na=False)]

        # Verificar y renombrar columnas necesarias
        if 'NOMBRE' in df_jefes.columns and 'No DOCUMENTO' in df_jefes.columns:
            df_jefes = df_jefes[['NOMBRE', 'No DOCUMENTO']].rename(columns={'NOMBRE': 'nombre', 'No DOCUMENTO': 'identificacion'})

            # Limpiar nombres
            df_jefes['nombre'] = df_jefes['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())

            # Asignar estado de registro
            if origen.lower() == "retiro":
                df_jefes['id_estado_registro'] = 4
            else:
                df_jefes['id_estado_registro'] = df_jefes['nombre'].apply(lambda x: 4 if '(VACANTE)' in x else 1)

            # Consultar registros existentes
            df_jefes_existentes = consultarTablasPlantaComercialHistorico('tb_planta_jefe')

            # Determinar nuevos registros y los que necesitan actualización
            df_jefes_nuevos = df_jefes[~df_jefes['nombre'].isin(df_jefes_existentes['nombre'])]

            # Cargar nuevos jefes en la base de datos
            if not df_jefes_nuevos.empty:
                df_jefes_nuevos['id_jefe'] = df_jefes_nuevos.apply(lambda row: generate_uuid().upper(), axis=1)
                df_jefes_nuevos['id_tipo_documento'] = 3
                df_jefes_nuevos['fecha_creacion'] = pd.Timestamp.now()
                df_jefes_nuevos['fecha_modificacion'] = pd.Timestamp.now()
                cargueDatosBD('tb_planta_jefe', df_jefes_nuevos)

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(preparacionCargueTablaJefe.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
def preparacionCargueTablaEspecialista(df_planta_comercial, origen):
    """
    Función que prepara y carga la tabla de jefes en la base de datos a partir del archivo de planta comercial.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial que contiene la información de los empleados.
        origen: Indica de qué hoja (tipo de red) provienen los datos (e.g., "retiro", "cavs", etc.).

    Retorna:
        None

    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    try:
        # Filtrar por cargo para encontrar jefes
        df_jefes = df_planta_comercial[df_planta_comercial['CARGO ACTUAL'].str.contains("Especialista", case=False, na=False)]

        # Verificar y renombrar columnas necesarias
        if 'NOMBRE' in df_jefes.columns and 'No DOCUMENTO' in df_jefes.columns:
            df_jefes = df_jefes[['NOMBRE', 'No DOCUMENTO']].rename(columns={'NOMBRE': 'nombre', 'No DOCUMENTO': 'identificacion'})

            # Limpiar nombres
            df_jefes['nombre'] = df_jefes['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())

            # Asignar estado de registro
            if origen.lower() == "retiro":
                df_jefes['id_estado_registro'] = 4
            else:
                df_jefes['id_estado_registro'] = df_jefes['nombre'].apply(lambda x: 4 if '(VACANTE)' in x else 1)

            # Consultar registros existentes
            df_jefes_existentes = consultarTablasPlantaComercialHistorico('tb_planta_especialista')

            # Determinar nuevos registros y los que necesitan actualización
            df_jefes_nuevos = df_jefes[~df_jefes['nombre'].isin(df_jefes_existentes['nombre'])]

            # Cargar nuevos jefes en la base de datos
            if not df_jefes_nuevos.empty:
                df_jefes_nuevos['id_especialista'] = df_jefes_nuevos.apply(lambda row: generate_uuid().upper(), axis=1)
                df_jefes_nuevos['id_tipo_documento'] = 3
                df_jefes_nuevos['fecha_creacion'] = pd.Timestamp.now()
                df_jefes_nuevos['fecha_modificacion'] = pd.Timestamp.now()
                cargueDatosBD('tb_planta_especialista', df_jefes_nuevos)

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(preparacionCargueTablaEspecialista.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
def preparacionCargueTablaJefeInteligenciaComercial(df_planta_comercial, origen):
    """
    Función que prepara y carga la tabla de jefes en la base de datos a partir del archivo de planta comercial.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial que contiene la información de los empleados.
        origen: Indica de qué hoja (tipo de red) provienen los datos (e.g., "retiro", "cavs", etc.).

    Retorna:
        None

    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    try:
        # Filtrar por cargo para encontrar jefes
        df_jefes = df_planta_comercial[df_planta_comercial['CARGO ACTUAL'].str.contains("Jefe", case=False, na=False)]

        # Verificar y renombrar columnas necesarias
        if 'NOMBRE' in df_jefes.columns and 'No DOCUMENTO' in df_jefes.columns:
            df_jefes = df_jefes[['NOMBRE', 'No DOCUMENTO']].rename(columns={'NOMBRE': 'nombre', 'No DOCUMENTO': 'identificacion'})

            # Limpiar nombres y convertir a mayúsculas
            df_jefes['nombre'] = df_jefes['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip().upper())  # Convertir a mayúsculas

            # Asignar estado de registro
            if origen.lower() == "inteligencia_comercial_retiro":
                df_jefes['id_estado_registro'] = 4
            else:
                df_jefes['id_estado_registro'] = df_jefes['nombre'].apply(lambda x: 4 if '(VACANTE)' in x else 1)

            # Consultar registros existentes
            df_jefes_existentes = consultarTablasPlantaComercialHistorico('tb_planta_inteligencia_comercial_jefe')

            # Determinar nuevos registros y los que necesitan actualización
            df_jefes_nuevos = df_jefes[~df_jefes['nombre'].isin(df_jefes_existentes['nombre'])]

            # Cargar nuevos jefes en la base de datos
            if not df_jefes_nuevos.empty:
                df_jefes_nuevos['id_jefe'] = df_jefes_nuevos.apply(lambda row: generate_uuid().upper(), axis=1)
                df_jefes_nuevos['id_tipo_documento'] = 3
                df_jefes_nuevos['fecha_creacion'] = pd.Timestamp.now()
                df_jefes_nuevos['fecha_modificacion'] = pd.Timestamp.now()
                cargueDatosBD('tb_planta_inteligencia_comercial_jefe', df_jefes_nuevos)

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(preparacionCargueTablaJefeInteligenciaComercial.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
def preparacionCargueTablaCoordinadorDirecto(df_planta_comercial, origen):
    """
    Función que prepara y carga la tabla de coordinadores directos en la base de datos a partir del archivo de planta comercial.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial que contiene la información de los empleados.

    Retorna:
        None

    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    try:
        # Filtrar por cargo para encontrar jefes
        df_coordinador = df_planta_comercial[df_planta_comercial['CARGO ACTUAL'].str.contains("Coordinador", case=False, na=False)]
        #print(f"Filtrados {len(df_coordinador)} coordinador en los datos de planta comercial.")

        # Verificar y renombrar columnas necesarias
        if 'NOMBRE' in df_coordinador.columns and 'No DOCUMENTO' in df_coordinador.columns:
            df_coordinador = df_coordinador[['NOMBRE', 'No DOCUMENTO']].rename(columns={'NOMBRE': 'nombre', 'No DOCUMENTO': 'identificacion'})

            # Limpiar nombres y asignar estado
            df_coordinador['nombre'] = df_coordinador['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
            
            if origen.lower() == "retiro":
                df_coordinador['id_estado_registro'] = 4
            else:
                df_coordinador['id_estado_registro'] = df_coordinador['nombre'].apply(lambda x: 4 if '(VACANTE)' in x else 1)

            # Consultar registros existentes
            df_coordinador_existentes = consultarTablasPlantaComercialHistorico('tb_planta_coordinador_directo')
            #print(f"Consultados {len(df_coordinador_existentes)} coordinador existentes en la base de datos.")

            # Determinar nuevos registros y los que necesitan actualización
            df_coordinador_nuevos = df_coordinador[~df_coordinador['nombre'].isin(df_coordinador_existentes['nombre'])]

            # Cargar nuevos coordinador en la base de datos
            if not df_coordinador_nuevos.empty:
                df_coordinador_nuevos['id_coordinador_directo'] = df_coordinador_nuevos.apply(lambda row: generate_uuid().upper(), axis=1)
                df_coordinador_nuevos['id_tipo_documento'] = 3
                df_coordinador_nuevos['fecha_creacion'] = pd.Timestamp.now()
                df_coordinador_nuevos['fecha_modificacion'] = pd.Timestamp.now()
                cargueDatosBD('tb_planta_coordinador_directo', df_coordinador_nuevos)

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(preparacionCargueTablaCoordinadorDirecto.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()    

# %%
def preparacionCargueTablaGerente(df_planta_comercial, origen):
    """
    Función que prepara y carga la tabla de gerentes en la base de datos a partir del archivo de planta comercial.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial que contiene la información de los empleados.

    Retorna:
        None

    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente.
    """
    try:
        # Filtrar por cargo para encontrar jefes
        df_gerente = df_planta_comercial[df_planta_comercial['CARGO ACTUAL'].str.contains("Gerente", case=False, na=False)]
        #print(f"Filtrados {len(df_gerente)} gerente en los datos de planta comercial.")

        # Verificar y renombrar columnas necesarias
        if 'NOMBRE' in df_gerente.columns and 'No DOCUMENTO' in df_gerente.columns:
            df_gerente = df_gerente[['NOMBRE', 'No DOCUMENTO']].rename(columns={'NOMBRE': 'nombre', 'No DOCUMENTO': 'identificacion'})

            # Limpiar nombres y asignar estado
            df_gerente['nombre'] = df_gerente['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
            # Asignar estado de registro
            if origen.lower() == "retiro":
                df_gerente['id_estado_registro'] = 4
            else:
                df_gerente['id_estado_registro'] = df_gerente['nombre'].apply(lambda x: 4 if '(VACANTE)' in x else 1)

            # Consultar registros existentes
            df_gerente_existentes = consultarTablasPlantaComercialHistorico('tb_planta_gerente')
            #print(f"Consultados {len(df_gerente_existentes)} gerente existentes en la base de datos.")

            # Determinar nuevos registros y los que necesitan actualización
            df_gerente_nuevos = df_gerente[~df_gerente['nombre'].isin(df_gerente_existentes['nombre'])]

            # Cargar nuevos gerente en la base de datos
            if not df_gerente_nuevos.empty:
                df_gerente_nuevos['id_gerente'] = df_gerente_nuevos.apply(lambda row: generate_uuid().upper(), axis=1)
                df_gerente_nuevos['id_tipo_documento'] = 3
                df_gerente_nuevos['fecha_creacion'] = pd.Timestamp.now()
                df_gerente_nuevos['fecha_modificacion'] = pd.Timestamp.now()
                cargueDatosBD('tb_planta_gerente', df_gerente_nuevos)
                
    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(preparacionCargueTablaGerente.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo() 

# %%
def manejarJefesVacantes(df_planta_comercial):
    """
    Función que maneja los registros de jefes vacantes en el DataFrame de planta comercial.
    Actualiza los registros en la base de datos si se encuentran jefes marcados como vacantes.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial que contiene la información de los empleados.

    Retorna:
        None

    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan actualizar los datos en la BD y genera un log localmente.
    """
    try:
        # Obtén la lista de nombres de jefes desde la base de datos
        df_jefes_existentes = consultarTablasPlantaComercialHistorico('tb_planta_jefe')

        df_jefes = df_planta_comercial[df_planta_comercial['CARGO ACTUAL'].str.contains("Jefe", case=False, na=False)]
        # Limpiar nombres en el DataFrame original para asegurar que las comparaciones sean correctas

        # Encuentra registros vacantes
        vacantes = df_jefes[df_jefes['NOMBRE'].str.contains('VACANTE')]
    

        vacantes['NOMBRE'] = vacantes['NOMBRE'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        # Filtrar por los que ya existen en la base de datos y necesitan actualización de estado
        jefes_para_actualizar = vacantes[vacantes['NOMBRE'].isin(df_jefes_existentes['nombre'])]
        
        # Actualizar cada jefe marcado como vacante
        for index, row in jefes_para_actualizar.iterrows():
            nombre_limpio = row['NOMBRE'].replace('(VACANTE)', '').strip()
            jefe_id = df_jefes_existentes.loc[df_jefes_existentes['nombre'] == nombre_limpio, 'id_jefe'].values[0]
            if jefe_id:
                fecha_modificacion =  pd.Timestamp.now()
                actualizarDatosBD('tb_planta_jefe', 'id_jefe' ,jefe_id, 4, fecha_modificacion)

        #print(f"{len(jefes_para_actualizar)} registros de jefes vacantes actualizados.")

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(manejarJefesVacantes.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo() 


# %%
def manejarCoordinadorDirectosVacantes(df_planta_comercial):
    """
    Función que maneja los registros de coordinadores directos vacantes en el DataFrame de planta comercial.
    Actualiza los registros en la base de datos si se encuentran coordinadores directos marcados como vacantes.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial que contiene la información de los empleados.

    Retorna:
        None

    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan actualizar los datos en la BD y genera un log localmente.
    """
    try:
        # Obtén la lista de nombres de jefes desde la base de datos
        df_coordinador_existentes = consultarTablasPlantaComercialHistorico('tb_planta_coordinador_directo')

        df_coordinador = df_planta_comercial[df_planta_comercial['CARGO ACTUAL'].str.contains("Coordinador", case=False, na=False)]
        # Limpiar nombres en el DataFrame original para asegurar que las comparaciones sean correctas

        # Encuentra registros vacantes
        vacantes = df_coordinador[df_coordinador['NOMBRE'].str.contains('VACANTE')]
    

        vacantes['NOMBRE'] = vacantes['NOMBRE'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        # Filtrar por los que ya existen en la base de datos y necesitan actualización de estado
        coordinador_para_actualizar = vacantes[vacantes['NOMBRE'].isin(df_coordinador_existentes['nombre'])]
        
        # Actualizar cada jefe marcado como vacante
        for index, row in coordinador_para_actualizar.iterrows():
            nombre_limpio = row['NOMBRE'].replace('(VACANTE)', '').strip()
            coordinador_id = df_coordinador_existentes.loc[df_coordinador_existentes['nombre'] == nombre_limpio, 'id_coordinador_directo'].values[0]
            if coordinador_id:
                fecha_modificacion =  pd.Timestamp.now()
                actualizarDatosBD('tb_planta_coordinador_directo', 'id_coordinador_directo' ,coordinador_id, 4, fecha_modificacion)

        print(f"{len(coordinador_para_actualizar)} registros de jefes vacantes actualizados.")

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(preparacionCargueTablaGerente.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo() 


# %%
def manejarGerentesVacantes(df_planta_comercial):
    """
    Función que maneja los registros de gerentes vacantes en el DataFrame de planta comercial.
    Actualiza los registros en la base de datos si se encuentran gerentes marcados como vacantes.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial que contiene la información de los empleados.

    Retorna:
        None

    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan actualizar los datos en la BD y genera un log localmente.
    """
    try:
        # Obtén la lista de nombres de jefes desde la base de datos
        df_gerente_existentes = consultarTablasPlantaComercialHistorico('tb_planta_gerente')

        df_gerente = df_planta_comercial[df_planta_comercial['CARGO ACTUAL'].str.contains("gerente", case=False, na=False)]
        # Limpiar nombres en el DataFrame original para asegurar que las comparaciones sean correctas

        # Encuentra registros vacantes
        vacantes = df_gerente[df_gerente['NOMBRE'].str.contains('VACANTE')]
    

        vacantes['NOMBRE'] = vacantes['NOMBRE'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        # Filtrar por los que ya existen en la base de datos y necesitan actualización de estado
        gerentes_para_actualizar = vacantes[vacantes['NOMBRE'].isin(df_gerente_existentes['nombre'])]
        
        # Actualizar cada jefe marcado como vacante
        for index, row in gerentes_para_actualizar.iterrows():
            nombre_limpio = row['NOMBRE'].replace('(VACANTE)', '').strip()
            gerentes_id = df_gerente_existentes.loc[df_gerente_existentes['nombre'] == nombre_limpio, 'id_gerente'].values[0]
            if gerentes_id:
                fecha_modificacion =  pd.Timestamp.now()
                actualizarDatosBD('tb_planta_gerente', 'id_gerente' ,gerentes_id, 4, fecha_modificacion)

        #print(f"{len(gerentes_para_actualizar)} registros de gerente vacantes actualizados.")

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(manejarGerentesVacantes.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo() 


# %%
def preparacionCargueTablasDominioRedMaestra(df_planta_comercial):

    """
    Función que se encarga de crear las tablas de dominio de la base de datos a partir del archivo
    planta comercial, estas tablas son creadas a partir de información nueva que no existe actualmente
    en los registros de las tablas involucradas 

    Argumentos:
        df_planta_comercial: base de excel de planta comercial
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        #Construir tablas de dominio de base planta comercial

        dataframes_resultantes = tablasDominio(df_planta_comercial, fecha_inicio_date)

        df_tipo_documento = dataframes_resultantes.get('TIPO DOCUMENTO')
        df_genero = dataframes_resultantes.get('GENERO')
        df_cargo = dataframes_resultantes.get('CARGO ACTUAL')
        df_operacion = dataframes_resultantes.get('OPERACION')
        df_contrato = dataframes_resultantes.get('CONTRATO')
        df_tipo_contratacion = dataframes_resultantes.get('TIPO DE CONTRATACION')
        df_contratante = dataframes_resultantes.get('CONTRATANTE')
        df_especialista = dataframes_resultantes.get('NOMBRE ESPECIALISTA')
        df_coordinador_tercero = dataframes_resultantes.get('NOMBRE COORDINADOR TERCERO')
        df_gerente = dataframes_resultantes.get('NOMBRE GERENTE')
        df_director_comercial = dataframes_resultantes.get('NOMBRE DIRECTOR COMERCIAL')
        df_direccion_comercial = dataframes_resultantes.get('DIRECCION COMERCIAL')
        df_segmento = dataframes_resultantes.get('SEGMENTO')
        df_gerencia_jefatura_comercial = dataframes_resultantes.get('GERENCIA COMERCIAL/ O JEFATURA')
        df_grupo_comercial = dataframes_resultantes.get('GRUPO COMERCIAL')
        df_area = dataframes_resultantes.get('AREA')
        df_canal = dataframes_resultantes.get('CANAL')
        df_categoria = dataframes_resultantes.get('CATEGORIA')
        df_categorizacion = dataframes_resultantes.get('CATEGORIZACION')
        df_proveedor = dataframes_resultantes.get('PROVEEDOR')
        df_regional = dataframes_resultantes.get('REGIONAL')
        df_codigo_dane = dataframes_resultantes.get('CODIGO DANE')
        df_antiguedad = dataframes_resultantes.get('ANTIGUEDAD')
        df_estado = dataframes_resultantes.get('ESTADO')
        df_jefe = dataframes_resultantes.get('NOMBRE JEFE')

        df_tipo_documento = df_tipo_documento.rename(columns={'TIPO DOCUMENTO' : 'tipo_documento'})
        df_genero = df_genero.rename(columns={'GENERO' : 'genero'})
        df_cargo = df_cargo.rename(columns={'CARGO ACTUAL' : 'cargo_actual'})
        df_operacion = df_operacion.rename(columns={'OPERACION' : 'operacion'})
        df_contrato = df_contrato.rename(columns={'CONTRATO' : 'contrato'})
        df_tipo_contratacion = df_tipo_contratacion.rename(columns={'TIPO DE CONTRATACION' : 'tipo_contratacion'})
        df_contratante = df_contratante.rename(columns={'CONTRATANTE' : 'contratante'})
        df_direccion_comercial = df_direccion_comercial.rename(columns={'DIRECCION COMERCIAL' : 'direccion_comercial'})
        df_segmento = df_segmento.rename(columns={'SEGMENTO' : 'segmento'})
        df_gerencia_jefatura_comercial = df_gerencia_jefatura_comercial.rename(columns={'GERENCIA COMERCIAL/ O JEFATURA' : 'gerencia_jefatura'})
        df_grupo_comercial = df_grupo_comercial.rename(columns={'GRUPO COMERCIAL' : 'grupo'})
        df_area = df_area.rename(columns={'AREA' : 'area'})
        df_canal = df_canal.rename(columns={'CANAL' : 'canal'})
        df_categoria = df_categoria.rename(columns={'CATEGORIA' : 'categoria'})
        df_categorizacion = df_categorizacion.rename(columns={'CATEGORIZACION' : 'categorizacion'})
        df_proveedor = df_proveedor.rename(columns={'PROVEEDOR' : 'proveedor'})
        df_regional = df_regional.rename(columns={'REGIONAL' : 'regional'})
        df_codigo_dane = df_codigo_dane.rename(columns={'CODIGO DANE' : 'codigo_dane'}) #revisar si realmente se necesita esta tabla 
        df_antiguedad = df_antiguedad.rename(columns={'ANTIGUEDAD' : 'antiguedad'})
        df_estado = df_estado.rename(columns={'ESTADO' : 'estado'})

        #Consultar tablas de dominio historicas registradas en base de datos
        diccionario_tablas_dominio_principales = {
            'tb_planta_tipo_documento',
            'tb_planta_genero',
            'tb_planta_cargo',
            'tb_planta_operacion',
            'tb_planta_contrato',
            'tb_planta_tipo_contratacion',
            'tb_planta_contratante',
            'tb_planta_direccion_comercial',
            'tb_planta_segmento',
            'tb_planta_gerencia_jefatura_comercial',
            'tb_planta_grupo_comercial',
            'tb_planta_area',
            'tb_planta_canal',
            'tb_planta_categoria',
            'tb_planta_categorizacion',
            'tb_planta_proveedor',
            'tb_planta_regional',
            'tb_planta_antiguedad',
            'tb_planta_estado'
        }

        resultados_tablas_dominio_principales = {}

        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_principales[nombre_tabla] = df
            
        df_tipo_documento_hist = resultados_tablas_dominio_principales['tb_planta_tipo_documento']
        df_tipo_documento_hist = df_tipo_documento_hist[['tipo_documento']]
        df_genero_hist = resultados_tablas_dominio_principales['tb_planta_genero']
        df_genero_hist = df_genero_hist[['genero']]
        df_cargo_hist = resultados_tablas_dominio_principales['tb_planta_cargo']
        df_cargo_hist = df_cargo_hist[['cargo_actual']]
        df_operacion_hist = resultados_tablas_dominio_principales['tb_planta_operacion']
        df_operacion_hist = df_operacion_hist [['operacion']]
        df_contrato_hist = resultados_tablas_dominio_principales['tb_planta_contrato']
        df_contrato_hist = df_contrato_hist[['contrato']]
        df_tipo_contratacion_hist = resultados_tablas_dominio_principales['tb_planta_tipo_contratacion']
        df_tipo_contratacion_hist = df_tipo_contratacion_hist [['tipo_contratacion']]
        df_contratante_hist = resultados_tablas_dominio_principales['tb_planta_contratante']
        df_contratante_hist = df_contratante_hist [['contratante']]
        df_direccion_comercial_hist = resultados_tablas_dominio_principales['tb_planta_direccion_comercial']
        df_direccion_comercial_hist = df_direccion_comercial_hist [['direccion_comercial']]
        df_segmento_hist = resultados_tablas_dominio_principales['tb_planta_segmento']
        df_segmento_hist = df_segmento_hist [['segmento']]
        df_gerencia_jefatura_comercial_hist = resultados_tablas_dominio_principales['tb_planta_gerencia_jefatura_comercial']
        df_gerencia_jefatura_comercial_hist = df_gerencia_jefatura_comercial_hist [['gerencia_jefatura']]
        df_grupo_comercial_hist = resultados_tablas_dominio_principales['tb_planta_grupo_comercial']
        df_grupo_comercial_hist = df_grupo_comercial_hist [['grupo']]
        df_area_hist = resultados_tablas_dominio_principales['tb_planta_area']
        df_area_hist = df_area_hist [['area']]
        df_canal_hist = resultados_tablas_dominio_principales['tb_planta_canal']
        df_canal_hist = df_canal_hist[['canal']]
        df_categoria_hist = resultados_tablas_dominio_principales['tb_planta_categoria']
        df_categoria_hist = df_categoria_hist [['categoria']]
        df_categorizacion_hist = resultados_tablas_dominio_principales['tb_planta_categorizacion']
        df_categorizacion_hist = df_categorizacion_hist [['categorizacion']]
        df_proveedor_hist = resultados_tablas_dominio_principales['tb_planta_proveedor']
        df_proveedor_hist = df_proveedor_hist[['proveedor']]
        df_regional_hist = resultados_tablas_dominio_principales['tb_planta_regional']
        df_regional_hist = df_regional_hist [['regional']]
        df_antiguedad_hist = resultados_tablas_dominio_principales['tb_planta_antiguedad']
        df_antiguedad_hist = df_antiguedad_hist [['antiguedad']]
        df_estado_hist = resultados_tablas_dominio_principales['tb_planta_estado']
        df_estado_hist = df_estado_hist[['estado']]

        #IDENTIFICAR LOS REGISTROS QUE SON NUEVOS Y NO EXISTEN ACTUALMENTE EN LA BASE DE DATOS PARA LAS TABLAS DE DOMINIO PRINCIPALES
        df_tipo_documento_nuevo = pd.merge(df_tipo_documento, df_tipo_documento_hist, left_on='tipo_documento', right_on= ['tipo_documento'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_tipo_documento_nuevo = df_tipo_documento_nuevo.drop('_merge', axis=1)
        df_genero_nuevo = pd.merge(df_genero, df_genero_hist, left_on='genero', right_on='genero', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_genero_nuevo = df_genero_nuevo.drop('_merge', axis=1)
        df_cargo_nuevo = pd.merge(df_cargo, df_cargo_hist, left_on='cargo_actual', right_on='cargo_actual', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_cargo_nuevo = df_cargo_nuevo.drop('_merge', axis=1)
        df_operacion_nuevo = pd.merge(df_operacion, df_operacion_hist, left_on='operacion', right_on='operacion', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_operacion_nuevo = df_operacion_nuevo.drop('_merge', axis=1)
        df_contrato_nuevo = pd.merge(df_contrato, df_contrato_hist, left_on='contrato', right_on='contrato', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_contrato_nuevo = df_contrato_nuevo.drop('_merge', axis=1)
        df_tipo_contratacion_nuevo = pd.merge(df_tipo_contratacion, df_tipo_contratacion_hist, left_on='tipo_contratacion', right_on='tipo_contratacion', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_tipo_contratacion_nuevo = df_tipo_contratacion_nuevo.drop('_merge', axis=1)
        df_contratante_nuevo = pd.merge(df_contratante, df_contratante_hist, left_on='contratante', right_on='contratante', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_contratante_nuevo = df_contratante_nuevo.drop('_merge', axis=1)
        df_direccion_comercial_nuevo = pd.merge(df_direccion_comercial, df_direccion_comercial_hist, left_on='direccion_comercial', right_on='direccion_comercial', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_direccion_comercial_nuevo = df_direccion_comercial_nuevo.drop('_merge', axis=1)
        df_segmento_nuevo = pd.merge(df_segmento, df_segmento_hist, left_on='segmento', right_on='segmento', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_segmento_nuevo = df_segmento_nuevo.drop('_merge', axis=1)
        df_gerencia_jefatura_comercial_nuevo = pd.merge(df_gerencia_jefatura_comercial, df_gerencia_jefatura_comercial_hist, left_on='gerencia_jefatura', right_on='gerencia_jefatura', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_gerencia_jefatura_comercial_nuevo = df_gerencia_jefatura_comercial_nuevo.drop('_merge', axis=1)
        df_grupo_comercial_nuevo = pd.merge(df_grupo_comercial, df_grupo_comercial_hist, left_on='grupo', right_on='grupo', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_grupo_comercial_nuevo = df_grupo_comercial_nuevo.drop('_merge', axis=1)
        df_area_nuevo = pd.merge(df_area, df_area_hist, left_on='area', right_on='area', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_area_nuevo = df_area_nuevo.drop('_merge', axis=1)
        df_canal_nuevo = pd.merge(df_canal, df_canal_hist, left_on='canal', right_on='canal', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_canal_nuevo = df_canal_nuevo.drop('_merge', axis=1)
        df_categoria_nuevo = pd.merge(df_categoria, df_categoria_hist, left_on='categoria', right_on='categoria', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_categoria_nuevo = df_categoria_nuevo.drop('_merge', axis=1)
        df_categorizacion_nuevo = pd.merge(df_categorizacion, df_categorizacion_hist, left_on='categorizacion', right_on='categorizacion', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_categorizacion_nuevo = df_categorizacion_nuevo.drop('_merge', axis=1)
        df_proveedor_nuevo = pd.merge(df_proveedor, df_proveedor_hist, left_on='proveedor', right_on='proveedor', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_proveedor_nuevo = df_proveedor_nuevo.drop('_merge', axis=1)
        df_regional_nuevo = pd.merge(df_regional, df_regional_hist, left_on='regional', right_on='regional', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_regional_nuevo = df_regional_nuevo.drop('_merge', axis=1)
        df_antiguedad_nuevo = pd.merge(df_antiguedad, df_antiguedad_hist, left_on='antiguedad', right_on='antiguedad', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_antiguedad_nuevo = df_antiguedad_nuevo.drop('_merge', axis=1)
        df_estado_nuevo = pd.merge(df_estado, df_estado_hist, left_on='estado', right_on='estado', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_estado_nuevo = df_estado_nuevo.drop('_merge', axis=1)


        diccionario_tablas_dominio_principal_cargue = {
        'tb_planta_tipo_documento': df_tipo_documento_nuevo,
        'tb_planta_genero': df_genero_nuevo,
        'tb_planta_cargo': df_cargo_nuevo,
        'tb_planta_operacion': df_operacion_nuevo,
        'tb_planta_contrato': df_contrato_nuevo,
        'tb_planta_tipo_contratacion': df_tipo_contratacion_nuevo,
        'tb_planta_contratante': df_contratante_nuevo,
        'tb_planta_direccion_comercial': df_direccion_comercial_nuevo,
        'tb_planta_segmento': df_segmento_nuevo,
        'tb_planta_gerencia_jefatura_comercial': df_gerencia_jefatura_comercial_nuevo,
        'tb_planta_grupo_comercial': df_grupo_comercial_nuevo,
        'tb_planta_area': df_area_nuevo,
        'tb_planta_canal': df_canal_nuevo,
        'tb_planta_categoria': df_categoria_nuevo,
        'tb_planta_categorizacion': df_categorizacion_nuevo,
        'tb_planta_proveedor': df_proveedor_nuevo,
        'tb_planta_regional': df_regional_nuevo,
        'tb_planta_antiguedad': df_antiguedad_nuevo,
        'tb_planta_estado': df_estado_nuevo
        }

        for nombre_tabla, df_final in diccionario_tablas_dominio_principal_cargue.items():
            if not df_final.empty:
                df_final['id_estado_registro'] = df_final['id_estado_registro'].astype(int)
                cargueDatosBD(nombre_tabla,df_final)

        #CONSULTAR NUEVAMENTE TABLAS DE DOMINIO CARGADAS Y ACTUALIZADAS PARA ASIGNACION FINAL A LAS TABLAS QUE LO REQUIEREN

        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_principales[nombre_tabla] = df
            
        df_tipo_documento_actualizada = resultados_tablas_dominio_principales['tb_planta_tipo_documento']
        df_direccion_comercial_actualizada = resultados_tablas_dominio_principales['tb_planta_direccion_comercial']



        # CONSULTAR HISTORICO DE TABLAS DE DOMINIO SECUNDARIAS DE CARGOS ASOCIADOS

        diccionario_tablas_dominio_secundarias = {
            'tb_planta_especialista',
            'tb_planta_coordinador_tercero',
            'tb_planta_coordinador_directo',
            'tb_planta_jefe',
            'tb_planta_gerente',
            'tb_planta_director',
        }

        resultados_tablas_dominio_secundarias = {}

        for nombre_tabla in diccionario_tablas_dominio_secundarias:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_secundarias[nombre_tabla] = df

        df_especialista_hist = resultados_tablas_dominio_secundarias['tb_planta_especialista']
        df_especialista_hist = df_especialista_hist[['nombre']]
        df_coordinador_tercero_hist = resultados_tablas_dominio_secundarias['tb_planta_coordinador_tercero']
        df_coordinador_tercero_hist = df_coordinador_tercero_hist[['nombre']]
        df_coordinador_directo_hist = resultados_tablas_dominio_secundarias['tb_planta_coordinador_directo']
        df_coordinador_directo_hist = df_coordinador_directo_hist[['nombre']]
        df_jefe_hist = resultados_tablas_dominio_secundarias['tb_planta_jefe']
        df_jefe_hist = df_jefe_hist[['nombre']]
        df_gerente_hist = resultados_tablas_dominio_secundarias['tb_planta_gerente']
        df_gerente_hist = df_gerente_hist[['nombre']]
        df_director_comercial_hist = resultados_tablas_dominio_secundarias['tb_planta_director']
        df_director_comercial_hist = df_director_comercial_hist[['nombre']]
        
        
        # PREPARAR LOS DATAFARMES ASOCIADOS A TABLAS DE DOMINIO SECUNDARIAS
        df_especialista = df_especialista.rename(columns={'NOMBRE ESPECIALISTA' : 'nombre'})
        df_especialista = pd.merge(df_especialista, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE'], how='left')
        df_especialista = df_especialista[['TIPO DOCUMENTO','No DOCUMENTO', 'nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_especialista = df_especialista.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        df_especialista = df_especialista.rename(columns={'TIPO DOCUMENTO' : 'tipo_documento',
                                                        'No DOCUMENTO' : 'identificacion',})
        # COnvierte los datos a int ya que estan en float para identificacion
        df_especialista['identificacion'] = df_especialista['identificacion'].fillna(0).astype(int)
        df_especialista = df_especialista.dropna(subset=['identificacion'])
        df_especialista['identificacion'] = df_especialista['identificacion'].astype(int)

        df_especialista['identificacion'] = df_especialista ['identificacion'].apply(lambda x: int(x))
        df_especialista = df_especialista.drop_duplicates()
        df_especialista['id_especialista'] = df_especialista.apply(lambda row: generate_uuid().upper(), axis=1)
        df_especialista = pd.merge(df_especialista, df_tipo_documento_actualizada, left_on='tipo_documento', right_on= ['tipo_documento'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_especialista = df_especialista.drop(['fecha_creacion_derecha','fecha_modificacion_derecha','id_estado_registro_derecha','_merge'], axis=1)
        df_especialista.columns = [col.replace('_izquierda', '') for col in df_especialista.columns]
        df_especialista = df_especialista [['id_especialista','id_tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        
        df_especialista['id_tipo_documento'] = df_especialista['id_tipo_documento'].fillna(3)
        
        df_especialista['identificacion'] = pd.to_numeric(df_especialista['identificacion'], errors='coerce').fillna(0)
        df_especialista['id_estado_registro'] = df_especialista.apply(lambda row: 4 if '(VACANTE)' in row['nombre'] else row['id_estado_registro'], axis=1)
        df_especialista['nombre'] = df_especialista['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_especialista = df_especialista.sort_values(by='id_estado_registro', ascending=True)
        df_especialista = df_especialista.drop_duplicates(subset=['nombre']).reset_index(drop=True)

        
        df_coordinador_tercero = df_coordinador_tercero.rename(columns={'NOMBRE COORDINADOR TERCERO' : 'nombre'})
        df_coordinador_tercero = pd.merge(df_coordinador_tercero, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE'], how='left')
        df_coordinador_tercero = df_coordinador_tercero[['TIPO DOCUMENTO','No DOCUMENTO', 'nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_coordinador_tercero = df_coordinador_tercero.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        
        df_coordinador_tercero = df_coordinador_tercero.rename(columns={'TIPO DOCUMENTO' : 'tipo_documento',
                                                        'No DOCUMENTO' : 'identificacion',})
        
        # COnvierte los datos a int ya que estan en float para identificacion
        df_coordinador_tercero['identificacion'] = df_coordinador_tercero['identificacion'].fillna(0).astype(int)
        df_coordinador_tercero = df_coordinador_tercero.dropna(subset=['identificacion'])
        df_coordinador_tercero['identificacion'] = df_coordinador_tercero['identificacion'].astype(int)
        
        df_coordinador_tercero['identificacion'] = df_coordinador_tercero ['identificacion'].apply(lambda x: int(x))
        df_coordinador_tercero = df_coordinador_tercero.drop_duplicates()

        df_coordinador_tercero['id_coordinador_tercero'] = df_coordinador_tercero.apply(lambda row: generate_uuid().upper(), axis=1)
        df_coordinador_tercero = pd.merge(df_coordinador_tercero, df_tipo_documento_actualizada, left_on='tipo_documento', right_on= ['tipo_documento'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_coordinador_tercero = df_coordinador_tercero.drop(['fecha_creacion_derecha','fecha_modificacion_derecha','id_estado_registro_derecha','_merge'], axis=1)
        df_coordinador_tercero.columns = [col.replace('_izquierda', '') for col in df_coordinador_tercero.columns]
        df_coordinador_tercero = df_coordinador_tercero [['id_coordinador_tercero','id_tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_coordinador_tercero['id_estado_registro'] = df_coordinador_tercero.apply(lambda row: 4 if '(VACANTE)' in row['nombre'] else row['id_estado_registro'], axis=1)
        df_coordinador_tercero['nombre'] = df_coordinador_tercero['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_coordinador_tercero = df_coordinador_tercero.sort_values(by='id_estado_registro', ascending=True)
        df_coordinador_tercero = df_coordinador_tercero.drop_duplicates(subset=['nombre']).reset_index(drop=True)

        df_director_comercial = df_director_comercial.rename(columns={'NOMBRE DIRECTOR COMERCIAL' : 'nombre'})
        df_director_comercial = pd.merge(df_director_comercial, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE'], how='left')
        df_director_comercial = df_director_comercial[['TIPO DOCUMENTO','No DOCUMENTO', 'nombre','fecha_creacion','fecha_modificacion','id_estado_registro','DIRECCION COMERCIAL']]
        df_director_comercial = df_director_comercial.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        df_director_comercial = df_director_comercial.rename(columns={'TIPO DOCUMENTO' : 'tipo_documento',
                                                        'No DOCUMENTO' : 'identificacion',
                                                        'DIRECCION COMERCIAL' : 'direccion_comercial'})
        
        # COnvierte los datos a int ya que estan en float para identificacion
        df_director_comercial['identificacion'] = df_director_comercial['identificacion'].fillna(0).astype(int)
        df_director_comercial = df_director_comercial.dropna(subset=['identificacion'])
        df_director_comercial['identificacion'] = df_director_comercial['identificacion'].astype(int)

        df_director_comercial['identificacion'] = df_director_comercial ['identificacion'].apply(lambda x: int(x))
        df_director_comercial = df_director_comercial.drop_duplicates()
        df_director_comercial['id_director'] = df_director_comercial.apply(lambda row: generate_uuid().upper(), axis=1)

        df_director_comercial = pd.merge(df_director_comercial, df_tipo_documento_actualizada, left_on='tipo_documento', right_on= ['tipo_documento'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_director_comercial = df_director_comercial.drop(['fecha_creacion_derecha','fecha_modificacion_derecha','id_estado_registro_derecha','_merge'], axis=1)
        df_director_comercial.columns = [col.replace('_izquierda', '') for col in df_director_comercial.columns]
        df_director_comercial = pd.merge(df_director_comercial, df_direccion_comercial_actualizada, left_on='direccion_comercial', right_on= ['direccion_comercial'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_director_comercial = df_director_comercial.drop(['fecha_creacion_derecha','fecha_modificacion_derecha','id_estado_registro_derecha','_merge'], axis=1)
        df_director_comercial.columns = [col.replace('_izquierda', '') for col in df_director_comercial.columns]
        df_director_comercial = df_director_comercial [['id_director','id_tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro','id_direccion_comercial']]
        
        df_director_comercial['id_direccion_comercial'] = df_director_comercial['id_direccion_comercial'].fillna(0)
        df_director_comercial['id_tipo_documento'] = df_director_comercial['id_tipo_documento'].fillna(3)
        
        df_director_comercial['id_estado_registro'] = df_director_comercial.apply(lambda row: 4 if '(VACANTE)' in row['nombre'] else row['id_estado_registro'], axis=1)
        df_director_comercial['nombre'] = df_director_comercial['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_director_comercial = df_director_comercial.sort_values(by='id_estado_registro', ascending=True)
        df_director_comercial = df_director_comercial.drop_duplicates(subset=['nombre']).reset_index(drop=True)

        df_jefe = df_jefe.rename(columns={'NOMBRE JEFE' : 'nombre'})
        df_jefe = pd.merge(df_jefe, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE'], how='left')
        df_jefe = df_jefe[['TIPO DOCUMENTO','No DOCUMENTO', 'nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_jefe = df_jefe.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        df_jefe_null_identificacion = df_jefe[df_jefe['No DOCUMENTO'].isnull()] #Se aplica este filtro para controlar el NA que genera la no identificación de MARIA LUISA ESCOLAR SUNDHEIM dentro de la RED Maestr, por ser la directora
        df_jefe_null_identificacion = df_jefe_null_identificacion.rename(columns={'No DOCUMENTO' : 'identificacion'}) #Se aplica este filtro para controlar el NA que genera la no identificación de MARIA LUISA ESCOLAR SUNDHEIM dentro de la RED Maestr, por ser la directora
        df_jefe = df_jefe[df_jefe['No DOCUMENTO'].notnull()]#Se aplica este filtro para controlar el NA que genera la no identificación de MARIA LUISA ESCOLAR SUNDHEIM dentro de la RED Maestr, por ser la directora
        df_jefe = df_jefe.rename(columns={'TIPO DOCUMENTO' : 'tipo_documento',
                                                        'No DOCUMENTO' : 'identificacion',})
        df_jefe['identificacion'] = df_jefe ['identificacion'].apply(lambda x: int(x))
        df_jefe = df_jefe.drop_duplicates()
        df_jefe['id_jefe'] = df_jefe.apply(lambda row: generate_uuid().upper(), axis=1)
        df_jefe = df_jefe [['id_jefe','tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        #SE APLICA ESTE PROCESO YA QUE  MARIA LUISA ESCOLAR SUNDHEIM NUNCA VA A APARECER EN LA RED MAESTRA POR SER LA JEFE DE TODOS
        #INICIO
        df_jefe_null_identificacion = df_jefe_null_identificacion[['nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_jefe_null_identificacion = pd.merge(df_jefe_null_identificacion, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE JEFE'], how='left')
        df_jefe_null_identificacion = df_jefe_null_identificacion.rename(columns={'DOCUMENTO JEFE' : 'identificacion'})
        df_jefe_null_identificacion ['tipo_documento'] = 'CC'
        df_jefe_null_identificacion = df_jefe_null_identificacion.drop_duplicates()
        df_jefe_null_identificacion = df_jefe_null_identificacion.head(1)
        df_jefe_null_identificacion['id_jefe'] = df_jefe_null_identificacion.apply(lambda row: generate_uuid().upper(), axis=1)
        df_jefe_null_identificacion = df_jefe_null_identificacion[['id_jefe','tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        #FIN
        df_jefe_final = pd.concat([df_jefe, df_jefe_null_identificacion])
        df_jefe_final = pd.merge(df_jefe_final, df_tipo_documento_actualizada, left_on='tipo_documento', right_on= ['tipo_documento'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_jefe_final = df_jefe_final.drop(['fecha_creacion_derecha','fecha_modificacion_derecha','id_estado_registro_derecha','_merge'], axis=1)
        df_jefe_final.columns = [col.replace('_izquierda', '') for col in df_jefe_final.columns]
        df_jefe_final = df_jefe_final[['id_jefe','id_tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_jefe_final['id_estado_registro'] = 5
        df_jefe_final['nombre'] = df_jefe_final['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_jefe_final = df_jefe_final.sort_values(by='id_estado_registro', ascending=True)
        df_jefe_final = df_jefe_final.drop_duplicates(subset=['nombre']).reset_index(drop=True)


        #IDENTIFICAR LOS REGISTROS QUE SON NUEVOS Y NO EXISTEN ACTUALMENTE EN LA BASE DE DATOS PARA LAS TABLAS DE DOMINIO SECUNDARIAS
        df_especialista_nuevo = pd.merge(df_especialista, df_especialista_hist, left_on='nombre', right_on= ['nombre'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_especialista_nuevo = df_especialista_nuevo.drop('_merge', axis=1)
        df_coordinador_tercero_nuevo = pd.merge(df_coordinador_tercero, df_coordinador_tercero_hist, left_on='nombre', right_on= ['nombre'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_coordinador_tercero_nuevo = df_coordinador_tercero_nuevo.drop('_merge', axis=1)
        df_director_comercial_nuevo = pd.merge(df_director_comercial, df_director_comercial_hist, left_on='nombre', right_on= ['nombre'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_director_comercial_nuevo = df_director_comercial_nuevo.drop('_merge', axis=1)
        df_jefe_nuevo= pd.merge(df_jefe_final, df_jefe_hist, left_on='nombre', right_on= ['nombre'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_jefe_nuevo = df_jefe_nuevo.drop('_merge', axis=1)


        
        diccionario_tablas_dominio_secundarias_cargue = {
        'tb_planta_especialista': df_especialista_nuevo,
        'tb_planta_coordinador_tercero': df_coordinador_tercero_nuevo,
        'tb_planta_director': df_director_comercial_nuevo,
        'tb_planta_jefe': df_jefe_nuevo
        }

        
        for nombre_tabla, df_final in diccionario_tablas_dominio_secundarias_cargue.items():
            if not df_final.empty:
                df_final['id_estado_registro'] = df_final['id_estado_registro'].astype(int)
                cargueDatosBD(nombre_tabla,df_final)
    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(preparacionCargueTablaCoordinadorDirecto.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()  


# %%
def preparacionCargueTablasDominioRetiro(df_planta_comercial):

    """
    Función que se encarga de crear las tablas de dominio de la base de datos a partir del archivo
    planta comercial, estas tablas son creadas a partir de información nueva que no existe actualmente
    en los registros de las tablas involucradas 

    Argumentos:
        df_planta_comercial: base de excel de planta comercial
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        #Construir tablas de dominio de base planta comercial

        dataframes_resultantes = tablasDominio(df_planta_comercial, fecha_inicio_date)

        df_tipo_documento = dataframes_resultantes.get('TIPO DOCUMENTO')
        df_genero = dataframes_resultantes.get('GENERO')
        df_cargo = dataframes_resultantes.get('CARGO ACTUAL')
        df_operacion = dataframes_resultantes.get('OPERACION')
        df_contrato = dataframes_resultantes.get('CONTRATO')
        df_tipo_contratacion = dataframes_resultantes.get('TIPO DE CONTRATACION')
        df_contratante = dataframes_resultantes.get('CONTRATANTE')
        df_especialista = dataframes_resultantes.get('NOMBRE ESPECIALISTA')
        df_coordinador_tercero = dataframes_resultantes.get('NOMBRE COORDINADOR TERCERO')
        df_gerente = dataframes_resultantes.get('NOMBRE GERENTE')
        df_gerente_documento = dataframes_resultantes.get('DOCUMENTO GERENTE')
        df_director_comercial = dataframes_resultantes.get('NOMBRE DIRECTOR COMERCIAL')
        df_direccion_comercial = dataframes_resultantes.get('DIRECCION COMERCIAL')
        df_segmento = dataframes_resultantes.get('SEGMENTO')
        df_gerencia_jefatura_comercial = dataframes_resultantes.get('GERENCIA COMERCIAL/ O JEFATURA')
        df_grupo_comercial = dataframes_resultantes.get('GRUPO COMERCIAL')
        df_area = dataframes_resultantes.get('AREA')
        df_canal = dataframes_resultantes.get('CANAL')
        df_categoria = dataframes_resultantes.get('CATEGORIA')
        df_categorizacion = dataframes_resultantes.get('CATEGORIZACION')
        df_proveedor = dataframes_resultantes.get('PROVEEDOR')
        df_regional = dataframes_resultantes.get('REGIONAL')
        df_codigo_dane = dataframes_resultantes.get('CODIGO DANE')
        df_antiguedad = dataframes_resultantes.get('ANTIGUEDAD')
        df_estado = dataframes_resultantes.get('ESTADO')
        df_jefe = dataframes_resultantes.get('NOMBRE JEFE')

        df_tipo_documento = df_tipo_documento.rename(columns={'TIPO DOCUMENTO' : 'tipo_documento'})
        df_genero = df_genero.rename(columns={'GENERO' : 'genero'})
        df_cargo = df_cargo.rename(columns={'CARGO ACTUAL' : 'cargo_actual'})
        df_operacion = df_operacion.rename(columns={'OPERACION' : 'operacion'})
        df_contrato = df_contrato.rename(columns={'CONTRATO' : 'contrato'})
        df_tipo_contratacion = df_tipo_contratacion.rename(columns={'TIPO DE CONTRATACION' : 'tipo_contratacion'})
        df_contratante = df_contratante.rename(columns={'CONTRATANTE' : 'contratante'})
        df_direccion_comercial = df_direccion_comercial.rename(columns={'DIRECCION COMERCIAL' : 'direccion_comercial'})
        df_segmento = df_segmento.rename(columns={'SEGMENTO' : 'segmento'})
        df_gerencia_jefatura_comercial = df_gerencia_jefatura_comercial.rename(columns={'GERENCIA COMERCIAL/ O JEFATURA' : 'gerencia_jefatura'})
        df_grupo_comercial = df_grupo_comercial.rename(columns={'GRUPO COMERCIAL' : 'grupo'})
        df_area = df_area.rename(columns={'AREA' : 'area'})
        df_canal = df_canal.rename(columns={'CANAL' : 'canal'})
        df_categoria = df_categoria.rename(columns={'CATEGORIA' : 'categoria'})
        df_categorizacion = df_categorizacion.rename(columns={'CATEGORIZACION' : 'categorizacion'})
        df_proveedor = df_proveedor.rename(columns={'PROVEEDOR' : 'proveedor'})
        df_regional = df_regional.rename(columns={'REGIONAL' : 'regional'})
        df_codigo_dane = df_codigo_dane.rename(columns={'CODIGO DANE' : 'codigo_dane'})  
        df_antiguedad = df_antiguedad.rename(columns={'ANTIGUEDAD' : 'antiguedad'})
        df_estado = df_estado.rename(columns={'ESTADO' : 'estado'})

        #Consultar tablas de dominio historicas registradas en base de datos
        diccionario_tablas_dominio_principales = {
            'tb_planta_tipo_documento',
            'tb_planta_genero',
            'tb_planta_cargo',
            'tb_planta_operacion',
            'tb_planta_contrato',
            'tb_planta_tipo_contratacion',
            'tb_planta_contratante',
            'tb_planta_direccion_comercial',
            'tb_planta_segmento',
            'tb_planta_gerencia_jefatura_comercial',
            'tb_planta_grupo_comercial',
            'tb_planta_area',
            'tb_planta_canal',
            'tb_planta_categoria',
            'tb_planta_categorizacion',
            'tb_planta_proveedor',
            'tb_planta_regional',
            'tb_planta_antiguedad',
            'tb_planta_estado'
        }

        resultados_tablas_dominio_principales = {}

        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_principales[nombre_tabla] = df
            
        df_tipo_documento_hist = resultados_tablas_dominio_principales['tb_planta_tipo_documento']
        df_tipo_documento_hist = df_tipo_documento_hist[['tipo_documento']]
        df_genero_hist = resultados_tablas_dominio_principales['tb_planta_genero']
        df_genero_hist = df_genero_hist[['genero']]
        df_cargo_hist = resultados_tablas_dominio_principales['tb_planta_cargo']
        df_cargo_hist = df_cargo_hist[['cargo_actual']]
        df_operacion_hist = resultados_tablas_dominio_principales['tb_planta_operacion']
        df_operacion_hist = df_operacion_hist [['operacion']]
        df_contrato_hist = resultados_tablas_dominio_principales['tb_planta_contrato']
        df_contrato_hist = df_contrato_hist[['contrato']]
        df_tipo_contratacion_hist = resultados_tablas_dominio_principales['tb_planta_tipo_contratacion']
        df_tipo_contratacion_hist = df_tipo_contratacion_hist [['tipo_contratacion']]
        df_contratante_hist = resultados_tablas_dominio_principales['tb_planta_contratante']
        df_contratante_hist = df_contratante_hist [['contratante']]
        df_direccion_comercial_hist = resultados_tablas_dominio_principales['tb_planta_direccion_comercial']
        df_direccion_comercial_hist = df_direccion_comercial_hist [['direccion_comercial']]
        df_segmento_hist = resultados_tablas_dominio_principales['tb_planta_segmento']
        df_segmento_hist = df_segmento_hist [['segmento']]
        df_gerencia_jefatura_comercial_hist = resultados_tablas_dominio_principales['tb_planta_gerencia_jefatura_comercial']
        df_gerencia_jefatura_comercial_hist = df_gerencia_jefatura_comercial_hist [['gerencia_jefatura']]
        df_grupo_comercial_hist = resultados_tablas_dominio_principales['tb_planta_grupo_comercial']
        df_grupo_comercial_hist = df_grupo_comercial_hist [['grupo']]
        df_area_hist = resultados_tablas_dominio_principales['tb_planta_area']
        df_area_hist = df_area_hist [['area']]
        df_canal_hist = resultados_tablas_dominio_principales['tb_planta_canal']
        df_canal_hist = df_canal_hist[['canal']]
        df_categoria_hist = resultados_tablas_dominio_principales['tb_planta_categoria']
        df_categoria_hist = df_categoria_hist [['categoria']]
        df_categorizacion_hist = resultados_tablas_dominio_principales['tb_planta_categorizacion']
        df_categorizacion_hist = df_categorizacion_hist [['categorizacion']]
        df_proveedor_hist = resultados_tablas_dominio_principales['tb_planta_proveedor']
        df_proveedor_hist = df_proveedor_hist[['proveedor']]
        df_regional_hist = resultados_tablas_dominio_principales['tb_planta_regional']
        df_regional_hist = df_regional_hist [['regional']]
        df_antiguedad_hist = resultados_tablas_dominio_principales['tb_planta_antiguedad']
        df_antiguedad_hist = df_antiguedad_hist [['antiguedad']]
        df_estado_hist = resultados_tablas_dominio_principales['tb_planta_estado']
        df_estado_hist = df_estado_hist[['estado']]

        #IDENTIFICAR LOS REGISTROS QUE SON NUEVOS Y NO EXISTEN ACTUALMENTE EN LA BASE DE DATOS PARA LAS TABLAS DE DOMINIO PRINCIPALES
        df_tipo_documento_nuevo = pd.merge(df_tipo_documento, df_tipo_documento_hist, left_on='tipo_documento', right_on= ['tipo_documento'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_tipo_documento_nuevo = df_tipo_documento_nuevo.drop('_merge', axis=1)
        df_genero_nuevo = pd.merge(df_genero, df_genero_hist, left_on='genero', right_on='genero', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_genero_nuevo = df_genero_nuevo.drop('_merge', axis=1)
        df_cargo_nuevo = pd.merge(df_cargo, df_cargo_hist, left_on='cargo_actual', right_on='cargo_actual', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_cargo_nuevo = df_cargo_nuevo.drop('_merge', axis=1)
        df_operacion_nuevo = pd.merge(df_operacion, df_operacion_hist, left_on='operacion', right_on='operacion', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_operacion_nuevo = df_operacion_nuevo.drop('_merge', axis=1)
        df_contrato_nuevo = pd.merge(df_contrato, df_contrato_hist, left_on='contrato', right_on='contrato', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_contrato_nuevo = df_contrato_nuevo.drop('_merge', axis=1)
        df_tipo_contratacion_nuevo = pd.merge(df_tipo_contratacion, df_tipo_contratacion_hist, left_on='tipo_contratacion', right_on='tipo_contratacion', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_tipo_contratacion_nuevo = df_tipo_contratacion_nuevo.drop('_merge', axis=1)
        df_contratante_nuevo = pd.merge(df_contratante, df_contratante_hist, left_on='contratante', right_on='contratante', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_contratante_nuevo = df_contratante_nuevo.drop('_merge', axis=1)
        df_direccion_comercial_nuevo = pd.merge(df_direccion_comercial, df_direccion_comercial_hist, left_on='direccion_comercial', right_on='direccion_comercial', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_direccion_comercial_nuevo = df_direccion_comercial_nuevo.drop('_merge', axis=1)
        df_segmento_nuevo = pd.merge(df_segmento, df_segmento_hist, left_on='segmento', right_on='segmento', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_segmento_nuevo = df_segmento_nuevo.drop('_merge', axis=1)
        df_gerencia_jefatura_comercial_nuevo = pd.merge(df_gerencia_jefatura_comercial, df_gerencia_jefatura_comercial_hist, left_on='gerencia_jefatura', right_on='gerencia_jefatura', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_gerencia_jefatura_comercial_nuevo = df_gerencia_jefatura_comercial_nuevo.drop('_merge', axis=1)
        df_grupo_comercial_nuevo = pd.merge(df_grupo_comercial, df_grupo_comercial_hist, left_on='grupo', right_on='grupo', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_grupo_comercial_nuevo = df_grupo_comercial_nuevo.drop('_merge', axis=1)
        df_area_nuevo = pd.merge(df_area, df_area_hist, left_on='area', right_on='area', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_area_nuevo = df_area_nuevo.drop('_merge', axis=1)
        df_canal_nuevo = pd.merge(df_canal, df_canal_hist, left_on='canal', right_on='canal', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_canal_nuevo = df_canal_nuevo.drop('_merge', axis=1)
        df_categoria_nuevo = pd.merge(df_categoria, df_categoria_hist, left_on='categoria', right_on='categoria', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_categoria_nuevo = df_categoria_nuevo.drop('_merge', axis=1)
        df_categorizacion_nuevo = pd.merge(df_categorizacion, df_categorizacion_hist, left_on='categorizacion', right_on='categorizacion', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_categorizacion_nuevo = df_categorizacion_nuevo.drop('_merge', axis=1)
        df_proveedor_nuevo = pd.merge(df_proveedor, df_proveedor_hist, left_on='proveedor', right_on='proveedor', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_proveedor_nuevo = df_proveedor_nuevo.drop('_merge', axis=1)
        df_regional_nuevo = pd.merge(df_regional, df_regional_hist, left_on='regional', right_on='regional', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_regional_nuevo = df_regional_nuevo.drop('_merge', axis=1)
        df_antiguedad_nuevo = pd.merge(df_antiguedad, df_antiguedad_hist, left_on='antiguedad', right_on='antiguedad', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_antiguedad_nuevo = df_antiguedad_nuevo.drop('_merge', axis=1)
        df_estado_nuevo = pd.merge(df_estado, df_estado_hist, left_on='estado', right_on='estado', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_estado_nuevo = df_estado_nuevo.drop('_merge', axis=1)


        diccionario_tablas_dominio_principal_cargue = {
        'tb_planta_tipo_documento': df_tipo_documento_nuevo,
        'tb_planta_genero': df_genero_nuevo,
        'tb_planta_cargo': df_cargo_nuevo,
        'tb_planta_operacion': df_operacion_nuevo,
        'tb_planta_contrato': df_contrato_nuevo,
        'tb_planta_tipo_contratacion': df_tipo_contratacion_nuevo,
        'tb_planta_contratante': df_contratante_nuevo,
        'tb_planta_direccion_comercial': df_direccion_comercial_nuevo,
        'tb_planta_segmento': df_segmento_nuevo,
        'tb_planta_gerencia_jefatura_comercial': df_gerencia_jefatura_comercial_nuevo,
        'tb_planta_grupo_comercial': df_grupo_comercial_nuevo,
        'tb_planta_area': df_area_nuevo,
        'tb_planta_canal': df_canal_nuevo,
        'tb_planta_categoria': df_categoria_nuevo,
        'tb_planta_categorizacion': df_categorizacion_nuevo,
        'tb_planta_proveedor': df_proveedor_nuevo,
        'tb_planta_regional': df_regional_nuevo,
        'tb_planta_antiguedad': df_antiguedad_nuevo,
        'tb_planta_estado': df_estado_nuevo
        }

        for nombre_tabla, df_final in diccionario_tablas_dominio_principal_cargue.items():
            if not df_final.empty:
                df_final['id_estado_registro'] = df_final['id_estado_registro'].astype(int)
                cargueDatosBD(nombre_tabla,df_final)

        #CONSULTAR NUEVAMENTE TABLAS DE DOMINIO CARGADAS Y ACTUALIZADAS PARA ASIGNACION FINAL A LAS TABLAS QUE LO REQUIEREN

        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_principales[nombre_tabla] = df
            
        df_tipo_documento_actualizada = resultados_tablas_dominio_principales['tb_planta_tipo_documento']
        df_direccion_comercial_actualizada = resultados_tablas_dominio_principales['tb_planta_direccion_comercial']



        # CONSULTAR HISTORICO DE TABLAS DE DOMINIO SECUNDARIAS DE CARGOS ASOCIADOS

        diccionario_tablas_dominio_secundarias = {
            'tb_planta_especialista',
            'tb_planta_coordinador_tercero',
            'tb_planta_coordinador_directo',
            'tb_planta_jefe',
            'tb_planta_gerente',
            'tb_planta_director',
        }

        resultados_tablas_dominio_secundarias = {}

        for nombre_tabla in diccionario_tablas_dominio_secundarias:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_secundarias[nombre_tabla] = df

        df_especialista_hist = resultados_tablas_dominio_secundarias['tb_planta_especialista']
        df_especialista_hist = df_especialista_hist[['nombre']]
        df_coordinador_tercero_hist = resultados_tablas_dominio_secundarias['tb_planta_coordinador_tercero']
        df_coordinador_tercero_hist = df_coordinador_tercero_hist[['nombre']]
        df_coordinador_directo_hist = resultados_tablas_dominio_secundarias['tb_planta_coordinador_directo']
        df_coordinador_directo_hist = df_coordinador_directo_hist[['nombre']]
        df_gerente_hist = resultados_tablas_dominio_secundarias['tb_planta_gerente']
        df_gerente_hist = df_gerente_hist[['nombre']]
        df_jefe_hist = resultados_tablas_dominio_secundarias['tb_planta_jefe']
        df_jefe_hist = df_jefe_hist[['nombre']]
        df_director_comercial_hist = resultados_tablas_dominio_secundarias['tb_planta_director']
        df_director_comercial_hist = df_director_comercial_hist[['nombre']]
        
        
        # PREPARAR LOS DATAFARMES ASOCIADOS A TABLAS DE DOMINIO SECUNDARIAS
        df_especialista = df_especialista.rename(columns={'NOMBRE ESPECIALISTA' : 'nombre'})
        df_especialista = pd.merge(df_especialista, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE'], how='left')
        df_especialista = df_especialista[['TIPO DOCUMENTO','No DOCUMENTO', 'nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_especialista = df_especialista.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        df_especialista = df_especialista.rename(columns={'TIPO DOCUMENTO' : 'tipo_documento',
                                                        'No DOCUMENTO' : 'identificacion',})
        # COnvierte los datos a int ya que estan en float para identificacion
        df_especialista['identificacion'] = df_especialista['identificacion'].fillna(0).astype(int)
        df_especialista = df_especialista.dropna(subset=['identificacion'])
        df_especialista['identificacion'] = df_especialista['identificacion'].astype(int)

        df_especialista['identificacion'] = df_especialista ['identificacion'].apply(lambda x: int(x))
        df_especialista = df_especialista.drop_duplicates()
        df_especialista['id_especialista'] = df_especialista.apply(lambda row: generate_uuid().upper(), axis=1)
        df_especialista = pd.merge(df_especialista, df_tipo_documento_actualizada, left_on='tipo_documento', right_on= ['tipo_documento'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_especialista = df_especialista.drop(['fecha_creacion_derecha','fecha_modificacion_derecha','id_estado_registro_derecha','_merge'], axis=1)
        df_especialista.columns = [col.replace('_izquierda', '') for col in df_especialista.columns]
        df_especialista = df_especialista [['id_especialista','id_tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        
        df_especialista['id_tipo_documento'] = df_especialista['id_tipo_documento'].fillna(3)
        
        df_especialista['identificacion'] = pd.to_numeric(df_especialista['identificacion'], errors='coerce').fillna(0)
        df_especialista['id_estado_registro'] = df_especialista.apply(lambda row: 4 if '(VACANTE)' in row['nombre'] else row['id_estado_registro'], axis=1)
        df_especialista['nombre'] = df_especialista['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_especialista = df_especialista.sort_values(by='id_estado_registro', ascending=True)
        df_especialista = df_especialista.drop_duplicates(subset=['nombre']).reset_index(drop=True)

        
        df_coordinador_tercero = df_coordinador_tercero.rename(columns={'NOMBRE COORDINADOR TERCERO' : 'nombre'})
        df_coordinador_tercero = pd.merge(df_coordinador_tercero, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE'], how='left')
        df_coordinador_tercero = df_coordinador_tercero[['TIPO DOCUMENTO','No DOCUMENTO', 'nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_coordinador_tercero = df_coordinador_tercero.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        
        df_coordinador_tercero = df_coordinador_tercero.rename(columns={'TIPO DOCUMENTO' : 'tipo_documento',
                                                        'No DOCUMENTO' : 'identificacion',})
        
        # COnvierte los datos a int ya que estan en float para identificacion
        df_coordinador_tercero['identificacion'] = df_coordinador_tercero['identificacion'].fillna(0).astype(int)
        df_coordinador_tercero = df_coordinador_tercero.dropna(subset=['identificacion'])
        df_coordinador_tercero['identificacion'] = df_coordinador_tercero['identificacion'].astype(int)
        
        df_coordinador_tercero['identificacion'] = df_coordinador_tercero ['identificacion'].apply(lambda x: int(x))
        df_coordinador_tercero = df_coordinador_tercero.drop_duplicates()

        df_coordinador_tercero['id_coordinador_tercero'] = df_coordinador_tercero.apply(lambda row: generate_uuid().upper(), axis=1)
        df_coordinador_tercero = pd.merge(df_coordinador_tercero, df_tipo_documento_actualizada, left_on='tipo_documento', right_on= ['tipo_documento'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_coordinador_tercero = df_coordinador_tercero.drop(['fecha_creacion_derecha','fecha_modificacion_derecha','id_estado_registro_derecha','_merge'], axis=1)
        df_coordinador_tercero.columns = [col.replace('_izquierda', '') for col in df_coordinador_tercero.columns]
        df_coordinador_tercero = df_coordinador_tercero [['id_coordinador_tercero','id_tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_coordinador_tercero['id_estado_registro'] = df_coordinador_tercero.apply(lambda row: 4 if '(VACANTE)' in row['nombre'] else row['id_estado_registro'], axis=1)
        df_coordinador_tercero['nombre'] = df_coordinador_tercero['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_coordinador_tercero = df_coordinador_tercero.sort_values(by='id_estado_registro', ascending=True)
        df_coordinador_tercero = df_coordinador_tercero.drop_duplicates(subset=['nombre']).reset_index(drop=True)

        df_director_comercial = df_director_comercial.rename(columns={'NOMBRE DIRECTOR COMERCIAL' : 'nombre'})
        df_director_comercial = pd.merge(df_director_comercial, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE'], how='left')
        df_director_comercial = df_director_comercial[['TIPO DOCUMENTO','No DOCUMENTO', 'nombre','fecha_creacion','fecha_modificacion','id_estado_registro','DIRECCION COMERCIAL']]
        df_director_comercial = df_director_comercial.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        df_director_comercial = df_director_comercial.rename(columns={'TIPO DOCUMENTO' : 'tipo_documento',
                                                        'No DOCUMENTO' : 'identificacion',
                                                        'DIRECCION COMERCIAL' : 'direccion_comercial'})
        
        # COnvierte los datos a int ya que estan en float para identificacion
        df_director_comercial['identificacion'] = df_director_comercial['identificacion'].fillna(0).astype(int)
        df_director_comercial = df_director_comercial.dropna(subset=['identificacion'])
        df_director_comercial['identificacion'] = df_director_comercial['identificacion'].astype(int)

        df_director_comercial['identificacion'] = df_director_comercial ['identificacion'].apply(lambda x: int(x))
        df_director_comercial = df_director_comercial.drop_duplicates()
        df_director_comercial['id_director'] = df_director_comercial.apply(lambda row: generate_uuid().upper(), axis=1)

        df_director_comercial = pd.merge(df_director_comercial, df_tipo_documento_actualizada, left_on='tipo_documento', right_on= ['tipo_documento'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_director_comercial = df_director_comercial.drop(['fecha_creacion_derecha','fecha_modificacion_derecha','id_estado_registro_derecha','_merge'], axis=1)
        df_director_comercial.columns = [col.replace('_izquierda', '') for col in df_director_comercial.columns]
        df_director_comercial = pd.merge(df_director_comercial, df_direccion_comercial_actualizada, left_on='direccion_comercial', right_on= ['direccion_comercial'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_director_comercial = df_director_comercial.drop(['fecha_creacion_derecha','fecha_modificacion_derecha','id_estado_registro_derecha','_merge'], axis=1)
        df_director_comercial.columns = [col.replace('_izquierda', '') for col in df_director_comercial.columns]
        df_director_comercial = df_director_comercial [['id_director','id_tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro','id_direccion_comercial']]
        
        df_director_comercial['id_direccion_comercial'] = df_director_comercial['id_direccion_comercial'].fillna(0)
        df_director_comercial['id_tipo_documento'] = df_director_comercial['id_tipo_documento'].fillna(3)
        
        df_director_comercial['id_estado_registro'] = df_director_comercial.apply(lambda row: 4 if '(VACANTE)' in row['nombre'] else row['id_estado_registro'], axis=1)
        df_director_comercial['nombre'] = df_director_comercial['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_director_comercial = df_director_comercial.sort_values(by='id_estado_registro', ascending=True)
        df_director_comercial = df_director_comercial.drop_duplicates(subset=['nombre']).reset_index(drop=True)

        #                                           GERENTE                                                    #
        # Paso 1: Renombrar y hacer el merge inicial
        df_gerente = df_gerente.rename(columns={'NOMBRE GERENTE': 'nombre'})
        df_gerente = pd.merge(df_gerente, df_planta_comercial, left_on='nombre', right_on=['NOMBRE'], how='left')
        df_gerente = df_gerente[['TIPO DOCUMENTO', 'No DOCUMENTO', 'nombre', 'fecha_creacion', 'fecha_modificacion', 'id_estado_registro']]
        df_gerente = df_gerente.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        df_gerente = df_gerente.rename(columns={'TIPO DOCUMENTO': 'tipo_documento', 'No DOCUMENTO': 'identificacion'})
        df_gerente['tipo_documento'] = 'NO APLICA'
        df_gerente['identificacion'] = df_gerente_documento['DOCUMENTO GERENTE']
        df_gerente['identificacion'] = df_gerente['identificacion'].apply(lambda x: int(x) if pd.notna(x) and isinstance(x, (int, float)) else x)

        # Paso 2: Identificar los gerentes que no están en la tabla
        df_gerente_missing = df_gerente[df_gerente['identificacion'].isnull()]
        df_gerente.loc[df_gerente_missing.index, 'id_estado_registro'] = 4

        # Paso 3: Asignar ID y preparar los datos finales
        df_gerente = df_gerente.drop_duplicates()
        df_gerente['id_gerente'] = df_gerente.apply(lambda row: generate_uuid().upper(), axis=1)
        df_gerente = pd.merge(df_gerente, df_tipo_documento_actualizada, left_on='tipo_documento', right_on=['tipo_documento'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_gerente = df_gerente.drop(['fecha_creacion_derecha', 'fecha_modificacion_derecha', 'id_estado_registro_derecha', '_merge'], axis=1)
        df_gerente.columns = [col.replace('_izquierda', '') for col in df_gerente.columns]
        df_gerente = df_gerente[['id_gerente', 'id_tipo_documento', 'identificacion', 'nombre', 'fecha_creacion', 'fecha_modificacion', 'id_estado_registro']]
        df_gerente['id_estado_registro'] = df_gerente.apply(lambda row: 4 if '(VACANTE)' in row['nombre'] else row['id_estado_registro'], axis=1)
        df_gerente['nombre'] = df_gerente['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_gerente = df_gerente.sort_values(by='id_estado_registro', ascending=True)
        df_gerente = df_gerente.drop_duplicates(subset=['nombre']).reset_index(drop=True)

        df_jefe = df_jefe.rename(columns={'NOMBRE JEFE' : 'nombre'})
        df_jefe = pd.merge(df_jefe, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE'], how='left')
        df_jefe = df_jefe[['TIPO DOCUMENTO','No DOCUMENTO', 'nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_jefe = df_jefe.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        df_jefe_null_identificacion = df_jefe[df_jefe['No DOCUMENTO'].isnull()] #Se aplica este filtro para controlar el NA que genera la no identificación de MARIA LUISA ESCOLAR SUNDHEIM dentro de la RED Maestr, por ser la directora
        df_jefe_null_identificacion = df_jefe_null_identificacion.rename(columns={'No DOCUMENTO' : 'identificacion'}) #Se aplica este filtro para controlar el NA que genera la no identificación de MARIA LUISA ESCOLAR SUNDHEIM dentro de la RED Maestr, por ser la directora
        df_jefe = df_jefe[df_jefe['No DOCUMENTO'].notnull()]#Se aplica este filtro para controlar el NA que genera la no identificación de MARIA LUISA ESCOLAR SUNDHEIM dentro de la RED Maestr, por ser la directora
        df_jefe = df_jefe.rename(columns={'TIPO DOCUMENTO' : 'tipo_documento',
                                                        'No DOCUMENTO' : 'identificacion',})
        df_jefe['identificacion'] = df_jefe ['identificacion'].apply(lambda x: int(x))
        df_jefe = df_jefe.drop_duplicates()
        df_jefe['id_jefe'] = df_jefe.apply(lambda row: generate_uuid().upper(), axis=1)
        df_jefe = df_jefe [['id_jefe','tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        #SE APLICA ESTE PROCESO YA QUE  MARIA LUISA ESCOLAR SUNDHEIM NUNCA VA A APARECER EN LA RED MAESTRA POR SER LA JEFE DE TODOS
        #INICIO
        df_jefe_null_identificacion = df_jefe_null_identificacion[['nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_jefe_null_identificacion = pd.merge(df_jefe_null_identificacion, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE JEFE'], how='left')
        df_jefe_null_identificacion = df_jefe_null_identificacion.rename(columns={'DOCUMENTO JEFE' : 'identificacion'})
        df_jefe_null_identificacion ['tipo_documento'] = 'CC'
        df_jefe_null_identificacion = df_jefe_null_identificacion.drop_duplicates()
        df_jefe_null_identificacion = df_jefe_null_identificacion.head(1)
        df_jefe_null_identificacion['id_jefe'] = df_jefe_null_identificacion.apply(lambda row: generate_uuid().upper(), axis=1)
        df_jefe_null_identificacion = df_jefe_null_identificacion[['id_jefe','tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        #FIN
        df_jefe_final = pd.concat([df_jefe, df_jefe_null_identificacion])
        df_jefe_final = pd.merge(df_jefe_final, df_tipo_documento_actualizada, left_on='tipo_documento', right_on= ['tipo_documento'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_jefe_final = df_jefe_final.drop(['fecha_creacion_derecha','fecha_modificacion_derecha','id_estado_registro_derecha','_merge'], axis=1)
        df_jefe_final.columns = [col.replace('_izquierda', '') for col in df_jefe_final.columns]
        df_jefe_final = df_jefe_final[['id_jefe','id_tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_jefe_final['id_estado_registro'] = 5
        df_jefe_final['nombre'] = df_jefe_final['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_jefe_final = df_jefe_final.sort_values(by='id_estado_registro', ascending=True)
        df_jefe_final = df_jefe_final.drop_duplicates(subset=['nombre']).reset_index(drop=True)



        #IDENTIFICAR LOS REGISTROS QUE SON NUEVOS Y NO EXISTEN ACTUALMENTE EN LA BASE DE DATOS PARA LAS TABLAS DE DOMINIO SECUNDARIAS
        df_especialista_nuevo = pd.merge(df_especialista, df_especialista_hist, left_on='nombre', right_on= ['nombre'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_especialista_nuevo = df_especialista_nuevo.drop('_merge', axis=1)
        df_coordinador_tercero_nuevo = pd.merge(df_coordinador_tercero, df_coordinador_tercero_hist, left_on='nombre', right_on= ['nombre'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_coordinador_tercero_nuevo = df_coordinador_tercero_nuevo.drop('_merge', axis=1)
        df_director_comercial_nuevo = pd.merge(df_director_comercial, df_director_comercial_hist, left_on='nombre', right_on= ['nombre'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_director_comercial_nuevo = df_director_comercial_nuevo.drop('_merge', axis=1)
        df_jefe_nuevo= pd.merge(df_jefe_final, df_jefe_hist, left_on='nombre', right_on= ['nombre'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_jefe_nuevo = df_jefe_nuevo.drop('_merge', axis=1)
        df_gerente_nuevo= pd.merge(df_gerente, df_gerente_hist, left_on='nombre', right_on= ['nombre'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_gerente_nuevo = df_gerente_nuevo.drop('_merge', axis=1)
        
        diccionario_tablas_dominio_secundarias_cargue = {
        'tb_planta_especialista': df_especialista_nuevo,
        'tb_planta_coordinador_tercero': df_coordinador_tercero_nuevo,
        'tb_planta_director': df_director_comercial_nuevo,
        'tb_planta_gerente': df_gerente_nuevo,
        'tb_planta_jefe': df_jefe_nuevo
        }

        
        for nombre_tabla, df_final in diccionario_tablas_dominio_secundarias_cargue.items():
            if not df_final.empty:
                df_final['id_estado_registro'] = df_final['id_estado_registro'].astype(int)
                cargueDatosBD(nombre_tabla,df_final)
        
        
    except Exception as e:
        fuentes.append(par.nombre_hoja_red_retiro)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(preparacionCargueTablasDominioRetiro.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()

# %%
def preparacionCargueTablasDominioRetail(df_planta_comercial):
    """
    Prepara y carga datos de gerentes de la tabla de planta comercial para el dominio Retail.

    Args:
    df_planta_comercial (DataFrame): DataFrame con los datos de la planta comercial.

    Returns:
    DataFrame: DataFrame con la información de gerentes procesada.
    """
    try:
        # Obtener los DataFrames resultantes del procesamiento inicial
        dataframes_resultantes = tablasDominioRetail(df_planta_comercial, fecha_inicio_date)
        df_gerente = dataframes_resultantes.get('GERENTE')
        
        if df_gerente is not None:
            if 'GERENTE' in df_gerente.columns:
                df_gerente = df_gerente.rename(columns={'GERENTE': 'nombre'})

            df_gerente['id_gerente'] = df_gerente.apply(lambda row: generate_uuid().upper(), axis=1)
            df_gerente['identificacion'] = 0
            df_gerente['id_tipo_documento'] = 3
            df_gerente['id_estado_registro'] = 1
            df_gerente['fecha_creacion'] = pd.Timestamp.now()
            df_gerente['fecha_modificacion'] = pd.Timestamp.now()

            # Cargar datos históricos
            df_gerente_hist = consultarTablasPlantaComercialHistorico('tb_planta_gerente')

            if df_gerente_hist is not None:
                # Realizar la unión y procesar nuevas y existentes entradas
                merged_df = df_gerente.merge(df_gerente_hist, on='nombre', how='left', indicator=True)
                nuevos_gerentes = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
                existentes_gerentes = merged_df[merged_df['_merge'] == 'both'].drop(columns=['_merge'])

                # Renombrar las columnas con sufijos _x a los nombres esperados
                nuevos_gerentes = nuevos_gerentes.rename(columns={
                    'id_gerente_x': 'id_gerente',
                    'identificacion_x': 'identificacion',
                    'id_tipo_documento_x': 'id_tipo_documento',
                    'id_estado_registro_x': 'id_estado_registro',
                    'fecha_creacion_x': 'fecha_creacion',
                    'fecha_modificacion_x': 'fecha_modificacion'
                })

                # Verificar si las columnas necesarias existen en nuevos_gerentes
                required_columns = ['id_gerente', 'nombre', 'identificacion', 'id_tipo_documento', 'id_estado_registro', 'fecha_creacion', 'fecha_modificacion']
                missing_columns = [col for col in required_columns if col not in nuevos_gerentes.columns]
                if missing_columns:
                    raise KeyError(f"Las siguientes columnas faltan en nuevos_gerentes: {missing_columns}")

                if not nuevos_gerentes.empty:
                    cargueDatosBD('tb_planta_gerente', nuevos_gerentes[required_columns])

                for index, row in existentes_gerentes.iterrows():
                    nombre_limpio = row['nombre'].replace('(VACANTE)', '').strip()
                    if 'id_gerente' in existentes_gerentes.columns:
                        gerentes_id = existentes_gerentes.loc[existentes_gerentes['nombre'] == nombre_limpio, 'id_gerente'].values[0]
                        if gerentes_id:
                            fecha_modificacion = pd.Timestamp.now()
                            actualizarDatosBD('tb_planta_gerente', 'id_gerente', gerentes_id, 4, fecha_modificacion)
            else:
                print("No historical data found for gerentes. Proceeding with new entries only.")
        else:
            print("No gerente data to process.")
    
    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(preparacionCargueTablasDominioRetail.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()
        
    return df_gerente

# %%
def preparacionCargueTablasDominiocavs(df_planta_comercial):

    """
    Función que se encarga de crear las tablas de dominio de la base de datos a partir del archivo
    planta comercial, estas tablas son creadas a partir de información nueva que no existe actualmente
    en los registros de las tablas involucradas 

    Argumentos:
        df_planta_comercial: base de excel de planta comercial
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        #Construir tablas de dominio de base planta comercial

        dataframes_resultantes = tablasDominio(df_planta_comercial, fecha_inicio_date)

        df_cargo = dataframes_resultantes.get('CARGO ACTUAL')
        df_especialista = dataframes_resultantes.get('NOMBRE ESPECIALISTA')
        df_coordinador_tercero = dataframes_resultantes.get('NOMBRE COORDINADOR TERCERO')
        df_gerente = dataframes_resultantes.get('NOMBRE GERENTE')
        df_gerente_documento = dataframes_resultantes.get('DOCUMENTO GERENTE')
        df_director_comercial = dataframes_resultantes.get('NOMBRE DIRECTOR COMERCIAL')
        df_direccion_comercial = dataframes_resultantes.get('DIRECCION COMERCIAL')
        df_segmento = dataframes_resultantes.get('SEGMENTO')
        df_gerencia_jefatura_comercial = dataframes_resultantes.get('GERENCIA COMERCIAL/ O JEFATURA')
        df_canal = dataframes_resultantes.get('CANAL')
        df_jefe = dataframes_resultantes.get('NOMBRE JEFE')

        df_cargo = df_cargo.rename(columns={'CARGO ACTUAL' : 'cargo_actual'})
        df_direccion_comercial = df_direccion_comercial.rename(columns={'DIRECCION COMERCIAL' : 'direccion_comercial'})
        df_segmento = df_segmento.rename(columns={'SEGMENTO' : 'segmento'})
        df_gerencia_jefatura_comercial = df_gerencia_jefatura_comercial.rename(columns={'GERENCIA COMERCIAL/ O JEFATURA' : 'gerencia_jefatura'})
        df_canal = df_canal.rename(columns={'CANAL' : 'canal'})

        #Consultar tablas de dominio historicas registradas en base de datos
        diccionario_tablas_dominio_principales = {
            'tb_planta_tipo_documento',
            'tb_planta_genero',
            'tb_planta_cargo',
            'tb_planta_operacion',
            'tb_planta_contrato',
            'tb_planta_tipo_contratacion',
            'tb_planta_contratante',
            'tb_planta_direccion_comercial',
            'tb_planta_segmento',
            'tb_planta_gerencia_jefatura_comercial',
            'tb_planta_grupo_comercial',
            'tb_planta_area',
            'tb_planta_canal',
            'tb_planta_categoria',
            'tb_planta_categorizacion',
            'tb_planta_proveedor',
            'tb_planta_regional',
            'tb_planta_antiguedad',
            'tb_planta_estado'
        }

        resultados_tablas_dominio_principales = {}

        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_principales[nombre_tabla] = df
            
        df_tipo_documento_hist = resultados_tablas_dominio_principales['tb_planta_tipo_documento']
        df_tipo_documento_hist = df_tipo_documento_hist[['tipo_documento']]
        df_genero_hist = resultados_tablas_dominio_principales['tb_planta_genero']
        df_genero_hist = df_genero_hist[['genero']]
        df_cargo_hist = resultados_tablas_dominio_principales['tb_planta_cargo']
        df_cargo_hist = df_cargo_hist[['cargo_actual']]
        df_operacion_hist = resultados_tablas_dominio_principales['tb_planta_operacion']
        df_operacion_hist = df_operacion_hist [['operacion']]
        df_contrato_hist = resultados_tablas_dominio_principales['tb_planta_contrato']
        df_contrato_hist = df_contrato_hist[['contrato']]
        df_tipo_contratacion_hist = resultados_tablas_dominio_principales['tb_planta_tipo_contratacion']
        df_tipo_contratacion_hist = df_tipo_contratacion_hist [['tipo_contratacion']]
        df_contratante_hist = resultados_tablas_dominio_principales['tb_planta_contratante']
        df_contratante_hist = df_contratante_hist [['contratante']]
        df_direccion_comercial_hist = resultados_tablas_dominio_principales['tb_planta_direccion_comercial']
        df_direccion_comercial_hist = df_direccion_comercial_hist [['direccion_comercial']]
        df_segmento_hist = resultados_tablas_dominio_principales['tb_planta_segmento']
        df_segmento_hist = df_segmento_hist [['segmento']]
        df_gerencia_jefatura_comercial_hist = resultados_tablas_dominio_principales['tb_planta_gerencia_jefatura_comercial']
        df_gerencia_jefatura_comercial_hist = df_gerencia_jefatura_comercial_hist [['gerencia_jefatura']]
        df_grupo_comercial_hist = resultados_tablas_dominio_principales['tb_planta_grupo_comercial']
        df_grupo_comercial_hist = df_grupo_comercial_hist [['grupo']]
        df_area_hist = resultados_tablas_dominio_principales['tb_planta_area']
        df_area_hist = df_area_hist [['area']]
        df_canal_hist = resultados_tablas_dominio_principales['tb_planta_canal']
        df_canal_hist = df_canal_hist[['canal']]
        df_categoria_hist = resultados_tablas_dominio_principales['tb_planta_categoria']
        df_categoria_hist = df_categoria_hist [['categoria']]
        df_categorizacion_hist = resultados_tablas_dominio_principales['tb_planta_categorizacion']
        df_categorizacion_hist = df_categorizacion_hist [['categorizacion']]
        df_proveedor_hist = resultados_tablas_dominio_principales['tb_planta_proveedor']
        df_proveedor_hist = df_proveedor_hist[['proveedor']]
        df_regional_hist = resultados_tablas_dominio_principales['tb_planta_regional']
        df_regional_hist = df_regional_hist [['regional']]
        df_antiguedad_hist = resultados_tablas_dominio_principales['tb_planta_antiguedad']
        df_antiguedad_hist = df_antiguedad_hist [['antiguedad']]
        df_estado_hist = resultados_tablas_dominio_principales['tb_planta_estado']
        df_estado_hist = df_estado_hist[['estado']]

        #IDENTIFICAR LOS REGISTROS QUE SON NUEVOS Y NO EXISTEN ACTUALMENTE EN LA BASE DE DATOS PARA LAS TABLAS DE DOMINIO PRINCIPALES

        df_cargo_nuevo = pd.merge(df_cargo, df_cargo_hist, left_on='cargo_actual', right_on='cargo_actual', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_cargo_nuevo = df_cargo_nuevo.drop('_merge', axis=1)
        df_direccion_comercial_nuevo = pd.merge(df_direccion_comercial, df_direccion_comercial_hist, left_on='direccion_comercial', right_on='direccion_comercial', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_direccion_comercial_nuevo = df_direccion_comercial_nuevo.drop('_merge', axis=1)
        df_segmento_nuevo = pd.merge(df_segmento, df_segmento_hist, left_on='segmento', right_on='segmento', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_segmento_nuevo = df_segmento_nuevo.drop('_merge', axis=1)
        df_gerencia_jefatura_comercial_nuevo = pd.merge(df_gerencia_jefatura_comercial, df_gerencia_jefatura_comercial_hist, left_on='gerencia_jefatura', right_on='gerencia_jefatura', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_gerencia_jefatura_comercial_nuevo = df_gerencia_jefatura_comercial_nuevo.drop('_merge', axis=1)
        df_canal_nuevo = pd.merge(df_canal, df_canal_hist, left_on='canal', right_on='canal', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_canal_nuevo = df_canal_nuevo.drop('_merge', axis=1)



        diccionario_tablas_dominio_principal_cargue = {
        'tb_planta_cargo': df_cargo_nuevo,
        'tb_planta_direccion_comercial': df_direccion_comercial_nuevo,
        'tb_planta_segmento': df_segmento_nuevo,
        'tb_planta_gerencia_jefatura_comercial': df_gerencia_jefatura_comercial_nuevo,
        'tb_planta_canal': df_canal_nuevo
        }

        for nombre_tabla, df_final in diccionario_tablas_dominio_principal_cargue.items():
            if not df_final.empty:
                df_final['id_estado_registro'] = df_final['id_estado_registro'].astype(int)
                cargueDatosBD(nombre_tabla,df_final)

        #CONSULTAR NUEVAMENTE TABLAS DE DOMINIO CARGADAS Y ACTUALIZADAS PARA ASIGNACION FINAL A LAS TABLAS QUE LO REQUIEREN

        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_principales[nombre_tabla] = df
            
        df_tipo_documento_actualizada = resultados_tablas_dominio_principales['tb_planta_tipo_documento']
        df_direccion_comercial_actualizada = resultados_tablas_dominio_principales['tb_planta_direccion_comercial']



        # CONSULTAR HISTORICO DE TABLAS DE DOMINIO SECUNDARIAS DE CARGOS ASOCIADOS

        diccionario_tablas_dominio_secundarias = {
            'tb_planta_especialista',
            'tb_planta_coordinador_tercero',
            'tb_planta_coordinador_directo',
            'tb_planta_jefe',
            'tb_planta_gerente',
            'tb_planta_director',
        }

        resultados_tablas_dominio_secundarias = {}

        for nombre_tabla in diccionario_tablas_dominio_secundarias:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_secundarias[nombre_tabla] = df

        df_especialista_hist = resultados_tablas_dominio_secundarias['tb_planta_especialista']
        df_especialista_hist = df_especialista_hist[['nombre']]
        df_coordinador_tercero_hist = resultados_tablas_dominio_secundarias['tb_planta_coordinador_tercero']
        df_coordinador_tercero_hist = df_coordinador_tercero_hist[['nombre']]
        df_coordinador_directo_hist = resultados_tablas_dominio_secundarias['tb_planta_coordinador_directo']
        df_coordinador_directo_hist = df_coordinador_directo_hist[['nombre']]
        df_gerente_hist = resultados_tablas_dominio_secundarias['tb_planta_gerente']
        df_gerente_hist = df_gerente_hist[['nombre']]
        df_jefe_hist = resultados_tablas_dominio_secundarias['tb_planta_jefe']
        df_jefe_hist = df_jefe_hist[['nombre']]
        df_director_comercial_hist = resultados_tablas_dominio_secundarias['tb_planta_director']
        df_director_comercial_hist = df_director_comercial_hist[['nombre']]
        
        df_especialista['TIPO DOCUMENTO']='NO APLICA'
        # PREPARAR LOS DATAFARMES ASOCIADOS A TABLAS DE DOMINIO SECUNDARIAS
        df_especialista = df_especialista.rename(columns={'NOMBRE ESPECIALISTA' : 'nombre'})
        df_especialista = pd.merge(df_especialista, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE'], how='left')
        df_especialista = df_especialista[['TIPO DOCUMENTO','No DOCUMENTO', 'nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_especialista = df_especialista.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        df_especialista = df_especialista.rename(columns={'TIPO DOCUMENTO' : 'tipo_documento',
                                                        'No DOCUMENTO' : 'identificacion',})
        # COnvierte los datos a int ya que estan en float para identificacion
        df_especialista['identificacion'] = df_especialista['identificacion'].fillna(0).astype(int)
        df_especialista = df_especialista.dropna(subset=['identificacion'])
        df_especialista['identificacion'] = df_especialista['identificacion'].astype(int)

        df_especialista['identificacion'] = df_especialista ['identificacion'].apply(lambda x: int(x))
        df_especialista = df_especialista.drop_duplicates()
        df_especialista['id_especialista'] = df_especialista.apply(lambda row: generate_uuid().upper(), axis=1)
        df_especialista = pd.merge(df_especialista, df_tipo_documento_actualizada, left_on='tipo_documento', right_on= ['tipo_documento'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_especialista = df_especialista.drop(['fecha_creacion_derecha','fecha_modificacion_derecha','id_estado_registro_derecha','_merge'], axis=1)
        df_especialista.columns = [col.replace('_izquierda', '') for col in df_especialista.columns]
        df_especialista = df_especialista [['id_especialista','id_tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        
        df_especialista['id_tipo_documento'] = df_especialista['id_tipo_documento'].fillna(3)
        
        df_especialista['identificacion'] = pd.to_numeric(df_especialista['identificacion'], errors='coerce').fillna(0)
        df_especialista['id_estado_registro'] = df_especialista.apply(lambda row: 4 if '(VACANTE)' in row['nombre'] else row['id_estado_registro'], axis=1)
        df_especialista['nombre'] = df_especialista['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_especialista = df_especialista.sort_values(by='id_estado_registro', ascending=True)
        df_especialista = df_especialista.drop_duplicates(subset=['nombre']).reset_index(drop=True)

        #                                           GERENTE                                                    #
        # Paso 1: Renombrar y hacer el merge inicial
        df_gerente['TIPO DOCUMENTO']='NO APLICA'
        df_gerente = df_gerente.rename(columns={'NOMBRE GERENTE': 'nombre'})
        df_gerente = pd.merge(df_gerente, df_planta_comercial, left_on='nombre', right_on=['NOMBRE'], how='left')
        df_gerente = df_gerente[['TIPO DOCUMENTO', 'No DOCUMENTO', 'nombre', 'fecha_creacion', 'fecha_modificacion', 'id_estado_registro']]
        df_gerente = df_gerente.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        df_gerente = df_gerente.rename(columns={'TIPO DOCUMENTO': 'tipo_documento', 'No DOCUMENTO': 'identificacion'})
        df_gerente['tipo_documento'] = 'NO APLICA'
        df_gerente['identificacion'] = df_gerente_documento['DOCUMENTO GERENTE']
        df_gerente['identificacion'] = df_gerente['identificacion'].apply(lambda x: int(x) if pd.notna(x) and isinstance(x, (int, float)) else x)

        # Paso 2: Identificar los gerentes que no están en la tabla
        df_gerente_missing = df_gerente[df_gerente['identificacion'].isnull()]
        df_gerente.loc[df_gerente_missing.index, 'id_estado_registro'] = 4

        # Paso 3: Asignar ID y preparar los datos finales
        df_gerente = df_gerente.drop_duplicates()
        df_gerente['id_gerente'] = df_gerente.apply(lambda row: generate_uuid().upper(), axis=1)
        df_gerente = pd.merge(df_gerente, df_tipo_documento_actualizada, left_on='tipo_documento', right_on=['tipo_documento'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_gerente = df_gerente.drop(['fecha_creacion_derecha', 'fecha_modificacion_derecha', 'id_estado_registro_derecha', '_merge'], axis=1)
        df_gerente.columns = [col.replace('_izquierda', '') for col in df_gerente.columns]
        df_gerente = df_gerente[['id_gerente', 'id_tipo_documento', 'identificacion', 'nombre', 'fecha_creacion', 'fecha_modificacion', 'id_estado_registro']]
        df_gerente['id_estado_registro'] = df_gerente.apply(lambda row: 4 if '(VACANTE)' in row['nombre'] else row['id_estado_registro'], axis=1)
        df_gerente['nombre'] = df_gerente['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_gerente = df_gerente.sort_values(by='id_estado_registro', ascending=True)
        df_gerente = df_gerente.drop_duplicates(subset=['nombre']).reset_index(drop=True)

        df_jefe['TIPO DOCUMENTO']='NO APLICA'
        df_jefe = df_jefe.rename(columns={'NOMBRE JEFE' : 'nombre'})
        df_jefe = pd.merge(df_jefe, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE'], how='left')
        df_jefe = df_jefe[['TIPO DOCUMENTO','No DOCUMENTO', 'nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_jefe = df_jefe.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        df_jefe_null_identificacion = df_jefe[df_jefe['No DOCUMENTO'].isnull()] #Se aplica este filtro para controlar el NA que genera la no identificación de MARIA LUISA ESCOLAR SUNDHEIM dentro de la RED Maestr, por ser la directora
        df_jefe_null_identificacion = df_jefe_null_identificacion.rename(columns={'No DOCUMENTO' : 'identificacion'}) #Se aplica este filtro para controlar el NA que genera la no identificación de MARIA LUISA ESCOLAR SUNDHEIM dentro de la RED Maestr, por ser la directora
        df_jefe = df_jefe[df_jefe['No DOCUMENTO'].notnull()]#Se aplica este filtro para controlar el NA que genera la no identificación de MARIA LUISA ESCOLAR SUNDHEIM dentro de la RED Maestr, por ser la directora
        df_jefe = df_jefe.rename(columns={'TIPO DOCUMENTO' : 'tipo_documento',
                                                        'No DOCUMENTO' : 'identificacion',})
        df_jefe['identificacion'] = df_jefe ['identificacion'].apply(lambda x: int(x))
        df_jefe = df_jefe.drop_duplicates()
        df_jefe['id_jefe'] = df_jefe.apply(lambda row: generate_uuid().upper(), axis=1)
        df_jefe = df_jefe [['id_jefe','tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        #SE APLICA ESTE PROCESO YA QUE  MARIA LUISA ESCOLAR SUNDHEIM NUNCA VA A APARECER EN LA RED MAESTRA POR SER LA JEFE DE TODOS
        #INICIO
        df_jefe_null_identificacion = df_jefe_null_identificacion[['nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_jefe_null_identificacion = pd.merge(df_jefe_null_identificacion, df_planta_comercial, left_on='nombre', right_on= ['NOMBRE JEFE'], how='left')
        df_jefe_null_identificacion = df_jefe_null_identificacion.rename(columns={'DOCUMENTO JEFE' : 'identificacion'})
        df_jefe_null_identificacion ['tipo_documento'] = 'CC'
        df_jefe_null_identificacion = df_jefe_null_identificacion.drop_duplicates()
        df_jefe_null_identificacion = df_jefe_null_identificacion.head(1)
        df_jefe_null_identificacion['id_jefe'] = df_jefe_null_identificacion.apply(lambda row: generate_uuid().upper(), axis=1)
        df_jefe_null_identificacion = df_jefe_null_identificacion[['id_jefe','tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        #FIN
        df_jefe_final = pd.concat([df_jefe, df_jefe_null_identificacion])
        df_jefe_final = pd.merge(df_jefe_final, df_tipo_documento_actualizada, left_on='tipo_documento', right_on= ['tipo_documento'], how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_jefe_final = df_jefe_final.drop(['fecha_creacion_derecha','fecha_modificacion_derecha','id_estado_registro_derecha','_merge'], axis=1)
        df_jefe_final.columns = [col.replace('_izquierda', '') for col in df_jefe_final.columns]
        df_jefe_final = df_jefe_final[['id_jefe','id_tipo_documento','identificacion','nombre','fecha_creacion','fecha_modificacion','id_estado_registro']]
        df_jefe_final['id_estado_registro'] = 5
        df_jefe_final['nombre'] = df_jefe_final['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_jefe_final = df_jefe_final.sort_values(by='id_estado_registro', ascending=True)
        df_jefe_final = df_jefe_final.drop_duplicates(subset=['nombre']).reset_index(drop=True)



        #IDENTIFICAR LOS REGISTROS QUE SON NUEVOS Y NO EXISTEN ACTUALMENTE EN LA BASE DE DATOS PARA LAS TABLAS DE DOMINIO SECUNDARIAS
        df_especialista_nuevo = pd.merge(df_especialista, df_especialista_hist, left_on='nombre', right_on= ['nombre'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_especialista_nuevo = df_especialista_nuevo.drop('_merge', axis=1)
        df_jefe_nuevo= pd.merge(df_jefe_final, df_jefe_hist, left_on='nombre', right_on= ['nombre'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_jefe_nuevo = df_jefe_nuevo.drop('_merge', axis=1)
        df_gerente_nuevo= pd.merge(df_gerente, df_gerente_hist, left_on='nombre', right_on= ['nombre'], how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_gerente_nuevo = df_gerente_nuevo.drop('_merge', axis=1)
        
        diccionario_tablas_dominio_secundarias_cargue = {
        'tb_planta_especialista': df_especialista_nuevo,
        'tb_planta_gerente': df_gerente_nuevo,
        'tb_planta_jefe': df_jefe_nuevo
        }

        
        for nombre_tabla, df_final in diccionario_tablas_dominio_secundarias_cargue.items():
            if not df_final.empty:
                df_final['id_estado_registro'] = df_final['id_estado_registro'].astype(int)
                cargueDatosBD(nombre_tabla,df_final)
        
        
    except Exception as e:
        fuentes.append(par.nombre_hoja_red_cavs)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(preparacionCargueTablasDominiocavs.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()

# %%
def preparacionCargueTablasDominioTmk(df_planta_comercial):

    """
    Función que se encarga de crear las tablas de dominio de la base de datos a partir del archivo
    planta comercial, estas tablas son creadas a partir de información nueva que no existe actualmente
    en los registros de las tablas involucradas 

    Argumentos:
        df_planta_comercial: base de excel de planta comercial
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        #Construir tablas de dominio de base planta comercial

        dataframes_resultantes = tablasDominio(df_planta_comercial, fecha_inicio_date)

        df_direccion_comercial = dataframes_resultantes.get('DIRECCION COMERCIAL')
        df_especialista = dataframes_resultantes.get('ESPECIALISTA')
        df_gerente = dataframes_resultantes.get('NOMBRE DE GERENTE')
        df_gerencia_jefatura_comercial = dataframes_resultantes.get('GERENCIA')
        df_director = dataframes_resultantes.get('NOMBRE DIRECTOR')
        df_operacion = dataframes_resultantes.get('OPERACIÓN')


        df_direccion_comercial = df_direccion_comercial.rename(columns={'DIRECCION COMERCIAL' : 'direccion_comercial'})
        df_especialista = df_especialista.rename(columns={'ESPECIALISTA' : 'especialista'})
        df_operacion = df_operacion.rename(columns={'OPERACIÓN' : 'operacion'})
        df_gerente = df_gerente.rename(columns={'NOMBRE DE GERENTE' : 'gerente'})
        df_gerencia_jefatura_comercial = df_gerencia_jefatura_comercial.rename(columns={'GERENCIA' : 'gerencia_jefatura'})
        df_director = df_director.rename(columns={'NOMBRE DIRECTOR' : 'director'})



        #Consultar tablas de dominio historicas registradas en base de datos
        diccionario_tablas_dominio_principales = {
            'tb_planta_tipo_documento',
            'tb_planta_genero',
            'tb_planta_cargo',
            'tb_planta_operacion',
            'tb_planta_contrato',
            'tb_planta_tipo_contratacion',
            'tb_planta_contratante',
            'tb_planta_direccion_comercial',
            'tb_planta_segmento',
            'tb_planta_gerencia_jefatura_comercial',
            'tb_planta_grupo_comercial',
            'tb_planta_area',
            'tb_planta_canal',
            'tb_planta_categoria',
            'tb_planta_categorizacion',
            'tb_planta_proveedor',
            'tb_planta_regional',
            'tb_planta_antiguedad',
            'tb_planta_estado'
        }

        resultados_tablas_dominio_principales = {}

        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_principales[nombre_tabla] = df
            
        df_operacion_hist = resultados_tablas_dominio_principales['tb_planta_operacion']
        df_operacion_hist = df_operacion_hist [['operacion']]
        df_direccion_comercial_hist = resultados_tablas_dominio_principales['tb_planta_direccion_comercial']
        df_direccion_comercial_hist = df_direccion_comercial_hist [['direccion_comercial']]
        df_gerencia_jefatura_comercial_hist = resultados_tablas_dominio_principales['tb_planta_gerencia_jefatura_comercial']
        df_gerencia_jefatura_comercial_hist = df_gerencia_jefatura_comercial_hist [['gerencia_jefatura']]
    

        

        #IDENTIFICAR LOS REGISTROS QUE SON NUEVOS Y NO EXISTEN ACTUALMENTE EN LA BASE DE DATOS PARA LAS TABLAS DE DOMINIO PRINCIPALES
        
        df_operacion_nuevo = pd.merge(df_operacion, df_operacion_hist, left_on='operacion', right_on='operacion', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_operacion_nuevo = df_operacion_nuevo.drop('_merge', axis=1)
        df_direccion_comercial_nuevo = pd.merge(df_direccion_comercial, df_direccion_comercial_hist, left_on='direccion_comercial', right_on='direccion_comercial', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_direccion_comercial_nuevo = df_direccion_comercial_nuevo.drop('_merge', axis=1)
        df_gerencia_jefatura_comercial_nuevo = pd.merge(df_gerencia_jefatura_comercial, df_gerencia_jefatura_comercial_hist, left_on='gerencia_jefatura', right_on='gerencia_jefatura', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_gerencia_jefatura_comercial_nuevo = df_gerencia_jefatura_comercial_nuevo.drop('_merge', axis=1)
        


        diccionario_tablas_dominio_principal_cargue = {
        'tb_planta_operacion': df_operacion_nuevo,
        'tb_planta_direccion_comercial': df_direccion_comercial_nuevo,
        'tb_planta_gerencia_jefatura_comercial': df_gerencia_jefatura_comercial_nuevo
        }

        for nombre_tabla, df_final in diccionario_tablas_dominio_principal_cargue.items():
            if not df_final.empty:
                df_final['id_estado_registro'] = df_final['id_estado_registro'].astype(int)
                cargueDatosBD(nombre_tabla,df_final)
                
    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(preparacionCargueTablasDominioTmk.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def PrepararTablaPrincipalHechoRedMaestra(df_planta_comercial):
    """
    Función que prepara la tabla principal de hechos para la red maestra de planta comercial. Consulta tablas de dominio históricas,
    renombra columnas, realiza limpieza de datos, y consolida los datos en un DataFrame estructurado para su carga en la base de datos.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial
    Retorna:
        df_tabla_hecho_planta: DataFrame de la tabla de hechos lista para carga
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        diccionario_tablas_dominio_principales = {
            'tb_planta_tipo_documento',
            'tb_planta_genero',
            'tb_planta_cargo',
            'tb_planta_operacion',
            'tb_planta_contrato',
            'tb_planta_tipo_contratacion',
            'tb_planta_contratante',
            'tb_planta_direccion_comercial',
            'tb_planta_segmento',
            'tb_planta_gerencia_jefatura_comercial',
            'tb_planta_grupo_comercial',
            'tb_planta_area',
            'tb_planta_canal',
            'tb_planta_categoria',
            'tb_planta_categorizacion',
            'tb_planta_proveedor',
            'tb_planta_regional',
            'tb_planta_antiguedad',
            'tb_planta_estado',
            'tb_municipio',
            'tb_departamento',
            'tb_planta_especialista',
            'tb_planta_coordinador_tercero',
            'tb_planta_coordinador_directo',
            'tb_planta_jefe',
            'tb_planta_gerente',
            'tb_planta_director',
            'tb_planta_fuente',
        }

        resultados_tablas_dominio = {}
        
        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio[nombre_tabla] = df

        df_tipo_documento_actual = resultados_tablas_dominio['tb_planta_tipo_documento']
        df_genero_actual = resultados_tablas_dominio['tb_planta_genero']
        df_cargo_actual = resultados_tablas_dominio['tb_planta_cargo']
        df_operacion_actual = resultados_tablas_dominio['tb_planta_operacion']
        df_contrato_actual = resultados_tablas_dominio['tb_planta_contrato']
        df_tipo_contratacion_actual = resultados_tablas_dominio['tb_planta_tipo_contratacion']
        df_contratante_actual = resultados_tablas_dominio['tb_planta_contratante']
        df_segmento_actual = resultados_tablas_dominio['tb_planta_segmento']
        df_gerencia_jefatura_comercial_actual = resultados_tablas_dominio['tb_planta_gerencia_jefatura_comercial']
        df_grupo_comercial_actual = resultados_tablas_dominio['tb_planta_grupo_comercial']
        df_area_actual = resultados_tablas_dominio['tb_planta_area']
        df_canal_actual = resultados_tablas_dominio['tb_planta_canal']
        df_categoria_actual = resultados_tablas_dominio['tb_planta_categoria']
        df_categorizacion_actual = resultados_tablas_dominio['tb_planta_categorizacion']
        df_proveedor_actual = resultados_tablas_dominio['tb_planta_proveedor']
        df_regional_actual = resultados_tablas_dominio['tb_planta_regional']
        df_antiguedad_actual = resultados_tablas_dominio['tb_planta_antiguedad']
        df_estado_actual = resultados_tablas_dominio['tb_planta_estado']
        df_especialista_actual = resultados_tablas_dominio['tb_planta_especialista']
        df_coordinador_tercero_actual = resultados_tablas_dominio['tb_planta_coordinador_tercero']
        df_coordinador_directo_actual = resultados_tablas_dominio['tb_planta_coordinador_directo']
        df_jefe_actual = resultados_tablas_dominio['tb_planta_jefe']
        df_gerente_actual = resultados_tablas_dominio['tb_planta_gerente']
        df_director_actual = resultados_tablas_dominio['tb_planta_director']
        df_direccion_comercial_actual = resultados_tablas_dominio['tb_planta_direccion_comercial']
        df_fuente = resultados_tablas_dominio['tb_planta_fuente']


        
        df_tabla_hecho_planta = df_planta_comercial.rename(columns={
            'TIPO DOCUMENTO': 'tipo_documento_hecho',
            'No DOCUMENTO': 'identificacion_hecho',
            'NOMBRE': 'nombre_completo',
            'GENERO': 'genero',
            'CELULAR': 'celular',
            'CORREO': 'correo',
            'NACIONALIDAD': 'nacionalidad',
            'CARGO ACTUAL': 'cargo_actual',
            'OPERACION': 'operacion',
            'CODIGO VENTAS MOVIL': 'codigo_ventas_movil',
            'CONTRATO': 'contrato',
            'TIPO DE CONTRATACION': 'tipo_contrato',
            'CONTRATANTE': 'contratante',
            'NOMBRE ESPECIALISTA': 'especialista',
            'NOMBRE COORDINADOR TERCERO': 'coordinador_tercero',
            'NOMBRE COORDINADOR DIRECTO': 'coordinador_directo',
            'NOMBRE JEFE': 'jefe',
            'NOMBRE GERENTE': 'gerente',
            'NOMBRE DIRECTOR COMERCIAL': 'director',
            'SEGMENTO': 'segmento',
            'GERENCIA COMERCIAL/ O JEFATURA': 'gerencia_jefatura',
            'GRUPO COMERCIAL': 'grupo',
            'AREA': 'area',
            'CANAL': 'canal',
            'CATEGORIA': 'categoria',
            'CATEGORIZACION': 'categorizacion',
            'PROVEEDOR': 'proveedor',
            'CIUDAD': 'municipio',
            'REGIONAL': 'regional',
            'DEPARTAMENTO': 'departamento',
            'FECHA INGRESO AREA': 'fecha_ingreso',
            'ANTIGUEDAD': 'antiguedad',
            'ESTADO': 'estado',
            'DIRECCION COMERCIAL': 'direccion_comercial',
            'OBSERVACION': 'observacion',
            'FUENTE' : 'fuente'
        })

        print(df_tabla_hecho_planta.columns)
        
        df_tabla_hecho_planta['area'] = df_tabla_hecho_planta['area'].fillna('NO APLICA')
        df_tabla_hecho_planta['coordinador_directo'] = df_tabla_hecho_planta['coordinador_directo'].fillna('NO APLICA')
        #df_tabla_hecho_planta['jefe'] = df_tabla_hecho_planta['jefe'].fillna('NO APLICA')
        df_tabla_hecho_planta['fecha_retiro_area']= pd.Timestamp('1900-01-01')

        #jefes_existentes = set(df_jefe_actual['nombre'])

        #unique_jefes_main = set(df_tabla_hecho_planta['jefe'].unique())
        #unique_jefes_domain = set(df_jefe_actual['nombre'].unique())

        # Step 2: Find names in the main DataFrame not present in the domain DataFrame
        #jefes_not_in_domain = unique_jefes_main - unique_jefes_domain

        # Step 3: Display the result
        #print(jefes_not_in_domain)
        
        #df_tabla_hecho_planta['jefe'] = df_tabla_hecho_planta['jefe'].apply(lambda x: x if x in jefes_existentes else 'NO APLICA')
        
        df_tabla_hecho_planta['id_estado_registro_hecho'] = 1
        df_tabla_hecho_planta = df_tabla_hecho_planta[['tipo_documento_hecho','identificacion_hecho','nombre_completo','genero','celular','correo','nacionalidad','cargo_actual','operacion',
                                                    'codigo_ventas_movil','contrato','tipo_contrato','contratante','especialista','coordinador_tercero','observacion',
                                                    'coordinador_directo','jefe','gerente','director','segmento','gerencia_jefatura','grupo','area','canal', 'direccion_comercial',
                                                    'categoria','categorizacion','proveedor','municipio','regional','departamento','fecha_ingreso','antiguedad',
                                                    'estado','id_estado_registro_hecho','fuente','fecha_retiro_area']]
        df_tabla_hecho_planta = limpiezaCamposString(df_tabla_hecho_planta)
        
        df_tabla_hecho_planta['id'] = [generate_uuid().upper() for _ in range(len(df_tabla_hecho_planta))]
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_documento_actual, left_on='tipo_documento_hecho', right_on= ['tipo_documento'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_genero_actual, left_on='genero', right_on= ['genero'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_cargo_actual, left_on='cargo_actual', right_on= ['cargo_actual'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_operacion_actual, left_on='operacion', right_on= ['operacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_contrato_actual, left_on='contrato', right_on= ['contrato'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_contratacion_actual, left_on='tipo_contrato', right_on= ['tipo_contratacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_contratante_actual, left_on='contratante', right_on= ['contratante'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_segmento_actual, left_on='segmento', right_on= ['segmento'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerencia_jefatura_comercial_actual, left_on='gerencia_jefatura', right_on= ['gerencia_jefatura'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_grupo_comercial_actual, left_on='grupo', right_on= ['grupo'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_area_actual, left_on='area', right_on= ['area'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_canal_actual, left_on='canal', right_on= ['canal'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_categoria_actual, left_on='categoria', right_on= ['categoria'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_categorizacion_actual, left_on='categorizacion', right_on= ['categorizacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_proveedor_actual, left_on='proveedor', right_on= ['proveedor'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_regional_actual, left_on='regional', right_on= ['regional'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_antiguedad_actual, left_on='antiguedad', right_on= ['antiguedad'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_estado_actual, left_on='estado', right_on= ['estado'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_fuente, left_on='fuente', right_on= ['fuente'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_direccion_comercial_actual, left_on='direccion_comercial', right_on= ['direccion_comercial'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        
        df_tabla_hecho_planta['id_direccion_comercial_pr'] = df_tabla_hecho_planta['id_direccion_comercial']
        df_tabla_hecho_planta['especialista'] = df_tabla_hecho_planta['especialista'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_especialista_actual, left_on='especialista', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre'], axis=1)
        df_tabla_hecho_planta['coordinador_tercero'] = df_tabla_hecho_planta['coordinador_tercero'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_coordinador_tercero_actual, left_on='coordinador_tercero', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','id_tipo_documento_left','id_tipo_documento_right','identificacion_right'], axis=1)
        df_tabla_hecho_planta['coordinador_directo'] = df_tabla_hecho_planta['coordinador_directo'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_coordinador_directo_actual, left_on='coordinador_directo', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','id_tipo_documento_left','id_tipo_documento_right'], axis=1)
        df_tabla_hecho_planta['jefe'] = df_tabla_hecho_planta['jefe'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_jefe_actual, left_on='jefe', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','identificacion_right'], axis=1)
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop_duplicates().reset_index(drop=True)
        df_tabla_hecho_planta['gerente'] = df_tabla_hecho_planta['gerente'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerente_actual, left_on='gerente', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','id_tipo_documento_left','id_tipo_documento_right'], axis=1)
        df_tabla_hecho_planta['director'] = df_tabla_hecho_planta['director'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_director_actual, left_on='director', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','identificacion_right'], axis=1)
        

        
        #Traformaciones para la tabla Hecho de planta comercial
        df_tabla_hecho_planta['id_estado_registro_hecho'] = df_tabla_hecho_planta.apply(lambda row: 4 if '(VACANTE)' in row['nombre_completo'] else row['id_estado_registro_hecho'], axis=1)
        df_tabla_hecho_planta['nombre_completo'] = df_tabla_hecho_planta['nombre_completo'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = df_tabla_hecho_planta.rename(columns={'id_tipo_documento_hecho' : 'id_tipo_documento',
                                                                    'identificacion_hecho' : 'identificacion',
                                                                    'id_estado_registro_hecho' : 'id_estado_registro'})
        
        #df_tabla_hecho_planta['identificacion'] = df_tabla_hecho_planta['identificacion'].fillna(0).astype(int).astype(str)
        # Ahora aplica la función 'ajustar_cedula'
        df_tabla_hecho_planta['identificacion'] = df_tabla_hecho_planta['identificacion'].apply(ajustar_cedula)
        
        hoy = datetime.today()
    
        # Añadir columnas con la fecha de hoy
        df_tabla_hecho_planta['fecha_creacion']  = hoy
        df_tabla_hecho_planta['fecha_modificacion'] = hoy

        # Añadir columna 'dealer' con los últimos 8 dígitos de la columna 'identificacion'
        df_tabla_hecho_planta['dealer'] = df_tabla_hecho_planta['identificacion'].astype(str).str[-8:]
        df_tabla_hecho_planta['id_direccion_comercial'] = df_tabla_hecho_planta['id_direccion_comercial_pr']
        
        df_tabla_hecho_planta['id_tipo_documento'] = df_tabla_hecho_planta['id_tipo_documento'].fillna(3).astype(int)
        df_tabla_hecho_planta = df_tabla_hecho_planta.dropna(subset=['id_tipo_documento'])
        df_tabla_hecho_planta['id_tipo_documento'] = df_tabla_hecho_planta['id_tipo_documento'].astype(int)

        print(df_tabla_hecho_planta)

        df_tabla_hecho_planta = df_tabla_hecho_planta[['id', 'id_tipo_documento', 'identificacion', 'nombre_completo', 'celular', 'correo', 'nacionalidad', 'codigo_ventas_movil', 
                                                    'fecha_ingreso', 'fecha_creacion', 'fecha_modificacion', 'dealer', 'id_estado_registro', 'id_genero', 'id_cargo', 'id_operacion', 
                                                    'id_contrato', 'id_tipo_contrato', 'id_contratante', 'id_especialista', 'id_coordinador_tercero', 'id_coordinador_directo','observacion',
                                                        'id_jefe', 'id_gerente', 'id_direccion_comercial', 'id_director', 'id_segmento', 'id_gerencia_jefatura', 'id_grupo', 'id_area', 
                                                        'id_canal', 'id_categoria', 'id_categorizacion', 'id_proveedor', 'departamento', 'municipio', 'id_regional', 'id_antiguedad', 'id_estado',
                                                        'id_fuente','fecha_retiro_area'
                                                        ]]
        
        
        
    
    except Exception as e:
        fuentes.append(par.nombre_archivo_planta_ajuste+" | "+par.nombre_hoja_red_maestra)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(PrepararTablaPrincipalHechoRedMaestra.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()

    return df_tabla_hecho_planta

# %%
def PrepararTablaPrincipalHechoRetail(df_planta_comercial):
    """
    Función que prepara la tabla principal de hechos para la red maestra de planta comercial. Consulta tablas de dominio históricas,
    renombra columnas, realiza limpieza de datos, y consolida los datos en un DataFrame estructurado para su carga en la base de datos.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial
    Retorna:
        df_tabla_hecho_planta: DataFrame de la tabla de hechos lista para carga
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        # Consultar tablas de dominio históricas registradas en base de datos
        diccionario_tablas_dominio_principales = {
            'tb_planta_tipo_documento',
            'tb_planta_genero',
            'tb_planta_cargo',
            'tb_planta_operacion',
            'tb_planta_contrato',
            'tb_planta_tipo_contratacion',
            'tb_planta_contratante',
            'tb_planta_direccion_comercial',
            'tb_planta_segmento',
            'tb_planta_gerencia_jefatura_comercial',
            'tb_planta_grupo_comercial',
            'tb_planta_area',
            'tb_planta_canal',
            'tb_planta_categoria',
            'tb_planta_categorizacion',
            'tb_planta_proveedor',
            'tb_planta_regional',
            'tb_planta_antiguedad',
            'tb_planta_estado',
            'tb_municipio',
            'tb_departamento',
            'tb_planta_especialista',
            'tb_planta_coordinador_tercero',
            'tb_planta_coordinador_directo',
            'tb_planta_jefe',
            'tb_planta_gerente',
            'tb_planta_director',
            'tb_planta_fuente',
        }

        resultados_tablas_dominio = {}
        
        # Consultar todas las tablas de dominio y almacenarlas en resultados_tablas_dominio
        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio[nombre_tabla] = df

        # Obtener las tablas específicas necesarias para mapeos
        df_tipo_documento_actual = resultados_tablas_dominio['tb_planta_tipo_documento']
        df_genero_actual = resultados_tablas_dominio['tb_planta_genero']
        df_cargo_actual = resultados_tablas_dominio['tb_planta_cargo']
        df_operacion_actual = resultados_tablas_dominio['tb_planta_operacion']
        df_contrato_actual = resultados_tablas_dominio['tb_planta_contrato']
        df_tipo_contratacion_actual = resultados_tablas_dominio['tb_planta_tipo_contratacion']
        df_contratante_actual = resultados_tablas_dominio['tb_planta_contratante']
        df_segmento_actual = resultados_tablas_dominio['tb_planta_segmento']
        df_gerencia_jefatura_comercial_actual = resultados_tablas_dominio['tb_planta_gerencia_jefatura_comercial']
        df_grupo_comercial_actual = resultados_tablas_dominio['tb_planta_grupo_comercial']
        df_area_actual = resultados_tablas_dominio['tb_planta_area']
        df_canal_actual = resultados_tablas_dominio['tb_planta_canal']
        df_categoria_actual = resultados_tablas_dominio['tb_planta_categoria']
        df_categorizacion_actual = resultados_tablas_dominio['tb_planta_categorizacion']
        df_proveedor_actual = resultados_tablas_dominio['tb_planta_proveedor']
        df_regional_actual = resultados_tablas_dominio['tb_planta_regional']
        df_antiguedad_actual = resultados_tablas_dominio['tb_planta_antiguedad']
        df_estado_actual = resultados_tablas_dominio['tb_planta_estado']
        df_especialista_actual = resultados_tablas_dominio['tb_planta_especialista']
        df_coordinador_tercero_actual = resultados_tablas_dominio['tb_planta_coordinador_tercero']
        df_coordinador_directo_actual = resultados_tablas_dominio['tb_planta_coordinador_directo']
        df_jefe_actual = resultados_tablas_dominio['tb_planta_jefe']
        df_gerente_actual = resultados_tablas_dominio['tb_planta_gerente']
        df_director_actual = resultados_tablas_dominio['tb_planta_director']
        df_direccion_comercial_actual = resultados_tablas_dominio['tb_planta_direccion_comercial']
        df_fuente = resultados_tablas_dominio['tb_planta_fuente']

        # Renombrar columnas de df_planta_comercial según especificaciones
        df_tabla_hecho_planta = df_planta_comercial.rename(columns={
            'NIT': 'nit',
            'NOMBRE': 'nombre_completo',
            'GERENTE': 'gerente',
            'ESPECIALISTA': 'especialista',
            'CIUDAD': 'municipio',
            'DEPARTAMENTO': 'departamento',
            'FUENTE': 'fuente'
        })

        # Inicializar id_estado_registro_hecho en 1
        df_tabla_hecho_planta['id_estado_registro_hecho'] = 1
        df_tabla_hecho_planta['fecha_ingreso']= pd.Timestamp('1900-01-01')
        df_tabla_hecho_planta['fecha_retiro_area']= pd.Timestamp('1900-01-01')
        df_tabla_hecho_planta['coordinador_tercero'] = 'NO APLICA'
        df_tabla_hecho_planta['direccion_comercial'] = 'NO APLICA'
        df_tabla_hecho_planta['jefe'] = 'NO APLICA'
        df_tabla_hecho_planta['director'] = 'NO APLICA'
        df_tabla_hecho_planta['segmento'] = 'NO APLICA'
        df_tabla_hecho_planta['correo'] = '0'
        df_tabla_hecho_planta['nacionalidad'] = '0'
        df_tabla_hecho_planta['codigo_ventas_movil'] = '0'
        df_tabla_hecho_planta['celular'] = 0
        df_tabla_hecho_planta['tipo_documento_hecho'] = 'NO APLICA'
        df_tabla_hecho_planta['identificacion_hecho'] = 0
        df_tabla_hecho_planta['genero'] = 'NO APLICA'
        df_tabla_hecho_planta['cargo_actual'] = 'NO APLICA'
        df_tabla_hecho_planta['operacion'] = 'NO APLICA'
        df_tabla_hecho_planta['contrato'] = 'NO APLICA'
        df_tabla_hecho_planta['tipo_contrato'] = 'NO APLICA'
        df_tabla_hecho_planta['contratante'] = 'NO APLICA'
        df_tabla_hecho_planta['gerencia_jefatura'] = 'NO APLICA'
        df_tabla_hecho_planta['antiguedad'] = 'NO APLICA'
        df_tabla_hecho_planta['grupo'] = 'NO APLICA'
        df_tabla_hecho_planta['area'] = 'NO APLICA'
        df_tabla_hecho_planta['canal'] ='NO APLICA'
        df_tabla_hecho_planta['categoria'] = 'NO APLICA'
        df_tabla_hecho_planta['categorizacion'] ='NO APLICA'
        df_tabla_hecho_planta['proveedor'] = 'NO APLICA'
        df_tabla_hecho_planta['regional'] = 'NO APLICA'
        df_tabla_hecho_planta['estado'] = 'NO APLICA'
        df_tabla_hecho_planta['fuente'] = 'RETAIL'

        NO_APLICA_ID = {
            'especialista': 'NO APLICA', 
            'coordinador_directo': 'NO APLICA'  
        }

        def asignar_id_especialista_coordinador(nombre):
            try:
                # Verificar si el nombre está vacío o es nulo
                if not nombre or pd.isna(nombre):
                    return NO_APLICA_ID['especialista'], NO_APLICA_ID['coordinador_directo']

                # Intentar obtener el ID de especialista
                df_especialista = df_especialista_actual[df_especialista_actual['nombre'].str.contains(nombre, case=False, na=False)]['nombre'].tolist()
                # Intentar obtener el ID de coordinador
                df_coordinador = df_coordinador_directo_actual[df_coordinador_directo_actual['nombre'].str.contains(nombre, case=False, na=False)]['nombre'].tolist()

                # Devolver los ID encontrados o 'NO APLICA' si no se encuentra ninguno
                id_especialista_final = df_especialista[0] if df_especialista else NO_APLICA_ID['especialista']
                id_coordinador_final = df_coordinador[0] if df_coordinador else NO_APLICA_ID['coordinador_directo']
            except IndexError:
                # Si hay un IndexError, asignar 'NO APLICA' a ambos
                id_especialista_final = NO_APLICA_ID['especialista']
                id_coordinador_final = NO_APLICA_ID['coordinador_directo']

            return id_especialista_final, id_coordinador_final

        # Aplicar la función y asignar los resultados a las nuevas columnas
        df_tabla_hecho_planta['especialista_id'], df_tabla_hecho_planta['coordinador_directo_id'] = zip(*df_tabla_hecho_planta['especialista'].apply(asignar_id_especialista_coordinador))

        # Seleccionar las columnas necesarias y aplicar limpieza
        df_tabla_hecho_planta = df_tabla_hecho_planta[['nombre_completo', 'especialista_id', 'departamento','gerente','municipio' , 'fuente', 'nit', 'id_estado_registro_hecho',
                                                    'fecha_ingreso','fecha_retiro_area', 'coordinador_directo_id','coordinador_tercero','jefe','director',
                                                    'correo', 'nacionalidad','codigo_ventas_movil','celular','identificacion_hecho','tipo_documento_hecho',
                                                    'genero','cargo_actual','operacion','contrato','tipo_contrato','contratante','gerencia_jefatura','segmento',
                                                    'antiguedad','grupo','area','canal','categoria','categorizacion','proveedor','regional','estado',
                                                    'direccion_comercial']]

        df_tabla_hecho_planta = limpiezaCamposString(df_tabla_hecho_planta)
        
        # Generar UUIDs únicos para cada fila en 'id'
        df_tabla_hecho_planta['id'] = [generate_uuid().upper() for _ in range(len(df_tabla_hecho_planta))]
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_documento_actual, left_on='tipo_documento_hecho', right_on= ['tipo_documento'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_genero_actual, left_on='genero', right_on= ['genero'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_cargo_actual, left_on='cargo_actual', right_on= ['cargo_actual'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_operacion_actual, left_on='operacion', right_on= ['operacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_contrato_actual, left_on='contrato', right_on= ['contrato'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_contratacion_actual, left_on='tipo_contrato', right_on= ['tipo_contratacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_contratante_actual, left_on='contratante', right_on= ['contratante'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_segmento_actual, left_on='segmento', right_on= ['segmento'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerencia_jefatura_comercial_actual, left_on='gerencia_jefatura', right_on= ['gerencia_jefatura'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_grupo_comercial_actual, left_on='grupo', right_on= ['grupo'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_area_actual, left_on='area', right_on= ['area'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_canal_actual, left_on='canal', right_on= ['canal'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_categoria_actual, left_on='categoria', right_on= ['categoria'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_categorizacion_actual, left_on='categorizacion', right_on= ['categorizacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_proveedor_actual, left_on='proveedor', right_on= ['proveedor'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_regional_actual, left_on='regional', right_on= ['regional'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_antiguedad_actual, left_on='antiguedad', right_on= ['antiguedad'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_estado_actual, left_on='estado', right_on= ['estado'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_fuente, left_on='fuente', right_on= ['fuente'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_direccion_comercial_actual, left_on='direccion_comercial', right_on= ['direccion_comercial'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        
        df_tabla_hecho_planta['id_direccion_comercial_pr'] = df_tabla_hecho_planta['id_direccion_comercial']
        df_tabla_hecho_planta['especialista_id'] = df_tabla_hecho_planta['especialista_id'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_especialista_actual, left_on='especialista_id', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre'], axis=1)
        df_tabla_hecho_planta['coordinador_tercero'] = df_tabla_hecho_planta['coordinador_tercero'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_coordinador_tercero_actual, left_on='coordinador_tercero', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','id_tipo_documento_left','id_tipo_documento_right','identificacion_right'], axis=1)
        df_tabla_hecho_planta['coordinador_directo_id'] = df_tabla_hecho_planta['coordinador_directo_id'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_coordinador_directo_actual, left_on='coordinador_directo_id', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','id_tipo_documento_left','id_tipo_documento_right'], axis=1)
        df_tabla_hecho_planta['jefe'] = df_tabla_hecho_planta['jefe'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_jefe_actual, left_on='jefe', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','identificacion_right'], axis=1)
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop_duplicates().reset_index(drop=True)
        df_tabla_hecho_planta['gerente'] = df_tabla_hecho_planta['gerente'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerente_actual, left_on='gerente', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','id_tipo_documento_left','id_tipo_documento_right'], axis=1)
        df_tabla_hecho_planta['director'] = df_tabla_hecho_planta['director'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_director_actual, left_on='director', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','identificacion_right'], axis=1)

        # Transformaciones finales para la tabla Hecho de planta comercial
        df_tabla_hecho_planta['id_estado_registro_hecho'] = df_tabla_hecho_planta.apply(lambda row: 4 if '(VACANTE)' in row['nombre_completo'] else row['id_estado_registro_hecho'], axis=1)
        df_tabla_hecho_planta['nombre_completo'] = df_tabla_hecho_planta['nombre_completo'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = df_tabla_hecho_planta.rename(columns={'id_tipo_documento_hecho' : 'id_tipo_documento',
                                                                    'identificacion_hecho' : 'identificacion',
                                                                    'id_estado_registro_hecho' : 'id_estado_registro'})
        # Añadir columnas de fechas actuales
        hoy = datetime.today()
        df_tabla_hecho_planta['fecha_creacion'] = hoy
        df_tabla_hecho_planta['fecha_modificacion'] = hoy
        
        # Añadir columna 'dealer' con los últimos 8 dígitos de la columna 'identificacion'
        df_tabla_hecho_planta['dealer'] = df_tabla_hecho_planta['identificacion'].astype(str).str[-8:]
        df_tabla_hecho_planta['id_direccion_comercial'] = df_tabla_hecho_planta['id_direccion_comercial_pr']
        df_tabla_hecho_planta['id_tipo_documento'] = df_tabla_hecho_planta['id_tipo_documento'].fillna(3).astype(int)
        df_tabla_hecho_planta = df_tabla_hecho_planta.dropna(subset=['id_tipo_documento'])
        df_tabla_hecho_planta['id_tipo_documento'] = df_tabla_hecho_planta['id_tipo_documento'].astype(int)
        
        # Seleccionar y ordenar columnas finales
        df_tabla_hecho_planta = df_tabla_hecho_planta[['id', 'id_tipo_documento', 'identificacion','nombre_completo','celular','correo','nacionalidad','fecha_creacion', 'fecha_modificacion', 
                                                    'id_estado_registro', 'codigo_ventas_movil','id_genero', 'id_cargo', 'id_operacion', 'id_contrato', 'id_tipo_contrato', 'id_contratante', 'id_especialista', 'id_coordinador_tercero', 'id_coordinador_directo',
                                                        'id_jefe', 'id_gerente', 'id_direccion_comercial', 'id_director', 'id_segmento', 'id_gerencia_jefatura', 'id_grupo', 'id_area', 
                                                        'id_canal', 'id_categoria', 'id_categorizacion', 'id_proveedor', 'departamento', 'municipio', 'id_regional', 'id_antiguedad', 'id_estado',
                                                        'id_fuente','nit','fecha_ingreso','fecha_retiro_area']]
        
        return df_tabla_hecho_planta
    except Exception as e:
        fuentes.append(par.nombre_archivo_planta_ajuste+" | "+par.nombre_hoja_red_retail)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(PrepararTablaPrincipalHechoRetail.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
def PrepararTablaPrincipalHechoRetiro(df_planta_comercial):

    """
    Función que prepara la tabla principal de hechos para la red maestra de planta comercial. Consulta tablas de dominio históricas,
    renombra columnas, realiza limpieza de datos, y consolida los datos en un DataFrame estructurado para su carga en la base de datos.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial
    Retorna:
        df_tabla_hecho_planta: DataFrame de la tabla de hechos lista para carga
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        # Consultar tablas de dominio históricas registradas en base de datos
        diccionario_tablas_dominio_principales = {
            'tb_planta_tipo_documento',
            'tb_planta_genero',
            'tb_planta_cargo',
            'tb_planta_operacion',
            'tb_planta_contrato',
            'tb_planta_tipo_contratacion',
            'tb_planta_contratante',
            'tb_planta_direccion_comercial',
            'tb_planta_segmento',
            'tb_planta_gerencia_jefatura_comercial',
            'tb_planta_grupo_comercial',
            'tb_planta_area',
            'tb_planta_canal',
            'tb_planta_categoria',
            'tb_planta_categorizacion',
            'tb_planta_proveedor',
            'tb_planta_regional',
            'tb_planta_antiguedad',
            'tb_planta_estado',
            'tb_municipio',
            'tb_departamento',
            'tb_planta_especialista',
            'tb_planta_coordinador_tercero',
            'tb_planta_coordinador_directo',
            'tb_planta_jefe',
            'tb_planta_gerente',
            'tb_planta_director',
            'tb_planta_fuente',
        }

        resultados_tablas_dominio = {}
        
        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio[nombre_tabla] = df

        df_tipo_documento_actual = resultados_tablas_dominio['tb_planta_tipo_documento']
        df_genero_actual = resultados_tablas_dominio['tb_planta_genero']
        df_cargo_actual = resultados_tablas_dominio['tb_planta_cargo']
        df_operacion_actual = resultados_tablas_dominio['tb_planta_operacion']
        df_contrato_actual = resultados_tablas_dominio['tb_planta_contrato']
        df_tipo_contratacion_actual = resultados_tablas_dominio['tb_planta_tipo_contratacion']
        df_contratante_actual = resultados_tablas_dominio['tb_planta_contratante']
        df_segmento_actual = resultados_tablas_dominio['tb_planta_segmento']
        df_gerencia_jefatura_comercial_actual = resultados_tablas_dominio['tb_planta_gerencia_jefatura_comercial']
        df_grupo_comercial_actual = resultados_tablas_dominio['tb_planta_grupo_comercial']
        df_area_actual = resultados_tablas_dominio['tb_planta_area']
        df_canal_actual = resultados_tablas_dominio['tb_planta_canal']
        df_categoria_actual = resultados_tablas_dominio['tb_planta_categoria']
        df_categorizacion_actual = resultados_tablas_dominio['tb_planta_categorizacion']
        df_proveedor_actual = resultados_tablas_dominio['tb_planta_proveedor']
        df_regional_actual = resultados_tablas_dominio['tb_planta_regional']
        df_antiguedad_actual = resultados_tablas_dominio['tb_planta_antiguedad']
        df_estado_actual = resultados_tablas_dominio['tb_planta_estado']
        df_especialista_actual = resultados_tablas_dominio['tb_planta_especialista']
        df_coordinador_tercero_actual = resultados_tablas_dominio['tb_planta_coordinador_tercero']
        df_coordinador_directo_actual = resultados_tablas_dominio['tb_planta_coordinador_directo']
        df_jefe_actual = resultados_tablas_dominio['tb_planta_jefe']
        df_gerente_actual = resultados_tablas_dominio['tb_planta_gerente']
        df_director_actual = resultados_tablas_dominio['tb_planta_director']
        df_direccion_comercial_actual = resultados_tablas_dominio['tb_planta_direccion_comercial']
        df_fuente = resultados_tablas_dominio['tb_planta_fuente']


        
        df_tabla_hecho_planta = df_planta_comercial.rename(columns={
            'TIPO DOCUMENTO': 'tipo_documento_hecho',
            'No DOCUMENTO': 'identificacion_hecho',
            'NOMBRE': 'nombre_completo',
            'GENERO': 'genero',
            'CELULAR': 'celular',
            'CORREO': 'correo',
            'NACIONALIDAD': 'nacionalidad',
            'CARGO ACTUAL': 'cargo_actual',
            'OPERACION': 'operacion',
            'CODIGO VENTAS MOVIL': 'codigo_ventas_movil',
            'CONTRATO': 'contrato',
            'TIPO DE CONTRATACION': 'tipo_contrato',
            'CONTRATANTE': 'contratante',
            'NOMBRE ESPECIALISTA': 'especialista',
            'NOMBRE COORDINADOR TERCERO': 'coordinador_tercero',
            'NOMBRE COORDINADOR DIRECTO': 'coordinador_directo',
            'NOMBRE JEFE': 'jefe',
            'NOMBRE GERENTE': 'gerente',
            'NOMBRE DIRECTOR COMERCIAL': 'director',
            'SEGMENTO': 'segmento',
            'GERENCIA COMERCIAL/ O JEFATURA': 'gerencia_jefatura',
            'GRUPO COMERCIAL': 'grupo',
            'AREA': 'area',
            'CANAL': 'canal',
            'CATEGORIA': 'categoria',
            'CATEGORIZACION': 'categorizacion',
            'PROVEEDOR': 'proveedor',
            'CIUDAD': 'municipio',
            'REGIONAL': 'regional',
            'DEPARTAMENTO': 'departamento',
            'FECHA INGRESO AREA': 'fecha_ingreso',
            'ANTIGUEDAD': 'antiguedad',
            'ESTADO': 'estado',
            'DIRECCION COMERCIAL': 'direccion_comercial',
            'FUENTE' : 'fuente',
            'OBSERVACION' : 'observacion',
            'FECHA RETIRO AREA' : 'fecha_retiro_area'
        })

        coordinador_directo_existentes = set(df_coordinador_directo_actual['nombre'])

        unique_coordinador_directo_main = set(df_tabla_hecho_planta['coordinador_directo'].unique())
        unique_coordinador_directo_domain = set(df_coordinador_directo_actual['nombre'].unique())
        # Step 2: Find names in the main DataFrame not present in the domain DataFrame
        coordinador_directo_not_in_domain = unique_coordinador_directo_main - unique_coordinador_directo_domain
        jefes_existentes = set(df_jefe_actual['nombre'])

        unique_jefes_main = set(df_tabla_hecho_planta['jefe'].unique())
        unique_jefes_domain = set(df_jefe_actual['nombre'].unique())
        # Step 2: Find names in the main DataFrame not present in the domain DataFrame
        jefes_not_in_domain = unique_jefes_main - unique_jefes_domain
      
        df_tabla_hecho_planta['jefe'] = df_tabla_hecho_planta['jefe'].apply(lambda x: x if x in jefes_existentes else 'NO APLICA')
        df_tabla_hecho_planta['coordinador_directo'] = df_tabla_hecho_planta['coordinador_directo'].apply(lambda x: x if x in coordinador_directo_existentes else 'NO APLICA')
            
        df_tabla_hecho_planta['id_estado_registro_hecho'] = 4
        df_tabla_hecho_planta = df_tabla_hecho_planta[['tipo_documento_hecho','identificacion_hecho','nombre_completo','genero','celular','correo','nacionalidad','cargo_actual','operacion',
                                                    'codigo_ventas_movil','contrato','tipo_contrato','contratante','especialista','coordinador_tercero','observacion',
                                                    'coordinador_directo','jefe','gerente','director','segmento','gerencia_jefatura','grupo','area','canal','direccion_comercial',
                                                    'categoria','categorizacion','proveedor','municipio','regional','departamento','fecha_ingreso','antiguedad',
                                                    'estado','id_estado_registro_hecho','fuente','fecha_retiro_area']]
        
        df_tabla_hecho_planta = limpiezaCamposString(df_tabla_hecho_planta)
        
        df_tabla_hecho_planta['id'] = [generate_uuid().upper() for _ in range(len(df_tabla_hecho_planta))]
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_documento_actual, left_on='tipo_documento_hecho', right_on= ['tipo_documento'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_genero_actual, left_on='genero', right_on= ['genero'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_cargo_actual, left_on='cargo_actual', right_on= ['cargo_actual'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_operacion_actual, left_on='operacion', right_on= ['operacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_contrato_actual, left_on='contrato', right_on= ['contrato'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_contratacion_actual, left_on='tipo_contrato', right_on= ['tipo_contratacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_contratante_actual, left_on='contratante', right_on= ['contratante'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_segmento_actual, left_on='segmento', right_on= ['segmento'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerencia_jefatura_comercial_actual, left_on='gerencia_jefatura', right_on= ['gerencia_jefatura'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_grupo_comercial_actual, left_on='grupo', right_on= ['grupo'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_area_actual, left_on='area', right_on= ['area'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_canal_actual, left_on='canal', right_on= ['canal'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_categoria_actual, left_on='categoria', right_on= ['categoria'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_categorizacion_actual, left_on='categorizacion', right_on= ['categorizacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_proveedor_actual, left_on='proveedor', right_on= ['proveedor'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_regional_actual, left_on='regional', right_on= ['regional'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_antiguedad_actual, left_on='antiguedad', right_on= ['antiguedad'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_estado_actual, left_on='estado', right_on= ['estado'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_fuente, left_on='fuente', right_on= ['fuente'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_direccion_comercial_actual, left_on='direccion_comercial', right_on= ['direccion_comercial'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        
        df_tabla_hecho_planta['id_direccion_comercial_pr'] = df_tabla_hecho_planta['id_direccion_comercial']
        
        df_tabla_hecho_planta['especialista'] = df_tabla_hecho_planta['especialista'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_especialista_actual, left_on='especialista', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre'], axis=1)
        df_tabla_hecho_planta['coordinador_tercero'] = df_tabla_hecho_planta['coordinador_tercero'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_coordinador_tercero_actual, left_on='coordinador_tercero', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','id_tipo_documento_left','id_tipo_documento_right','identificacion_right'], axis=1)
        df_tabla_hecho_planta['coordinador_directo'] = df_tabla_hecho_planta['coordinador_directo'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_coordinador_directo_actual, left_on='coordinador_directo', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','id_tipo_documento_left','id_tipo_documento_right'], axis=1)
        df_tabla_hecho_planta['jefe'] = df_tabla_hecho_planta['jefe'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_jefe_actual, left_on='jefe', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','identificacion_right'], axis=1)
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop_duplicates().reset_index(drop=True)
        df_tabla_hecho_planta['gerente'] = df_tabla_hecho_planta['gerente'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerente_actual, left_on='gerente', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','id_tipo_documento_left','id_tipo_documento_right'], axis=1)
        df_tabla_hecho_planta['director'] = df_tabla_hecho_planta['director'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_director_actual, left_on='director', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','identificacion_right'], axis=1)
        


        
        #Traformaciones para la tabla Hecho de planta comercial

        df_tabla_hecho_planta['id_estado_registro_hecho'] = df_tabla_hecho_planta.apply(lambda row: 4 if '(VACANTE)' in row['nombre_completo'] else row['id_estado_registro_hecho'], axis=1)
        df_tabla_hecho_planta['nombre_completo'] = df_tabla_hecho_planta['nombre_completo'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = df_tabla_hecho_planta.rename(columns={'id_tipo_documento_hecho' : 'id_tipo_documento',
                                                                    'identificacion_hecho' : 'identificacion',
                                                                    'id_estado_registro_hecho' : 'id_estado_registro'})
        
        hoy = datetime.today()
    
        #print(df_tabla_hecho_planta.dtypes)
        # Añadir columnas con la fecha de hoy
        df_tabla_hecho_planta['fecha_creacion']  = hoy
        df_tabla_hecho_planta['fecha_modificacion'] = hoy

        # Añadir columna 'dealer' con los últimos 8 dígitos de la columna 'identificacion'
        df_tabla_hecho_planta['dealer'] = df_tabla_hecho_planta['identificacion'].astype(str).str[-8:]
        df_tabla_hecho_planta['id_direccion_comercial'] = df_tabla_hecho_planta['id_direccion_comercial_pr']
        df_tabla_hecho_planta = df_tabla_hecho_planta.loc[:, ~df_tabla_hecho_planta.columns.duplicated()]
        df_tabla_hecho_planta['id_tipo_documento'] = df_tabla_hecho_planta['id_tipo_documento'].fillna(3).astype(int)
        df_tabla_hecho_planta = df_tabla_hecho_planta.dropna(subset=['id_tipo_documento'])
        df_tabla_hecho_planta['id_tipo_documento'] = df_tabla_hecho_planta['id_tipo_documento'].astype(int)
        

        df_tabla_hecho_planta = df_tabla_hecho_planta[['id', 'id_tipo_documento', 'identificacion', 'nombre_completo', 'celular', 'correo', 'nacionalidad', 'codigo_ventas_movil', 
                                                    'fecha_ingreso', 'fecha_creacion', 'fecha_modificacion', 'dealer', 'id_estado_registro', 'id_genero', 'id_cargo', 'id_operacion', 
                                                    'id_contrato', 'id_tipo_contrato', 'id_contratante', 'id_especialista', 'id_coordinador_tercero', 'id_coordinador_directo',
                                                    'id_jefe', 'id_gerente', 'id_direccion_comercial', 'id_director', 'id_segmento', 'id_gerencia_jefatura', 'id_grupo', 'id_area', 
                                                    'id_canal', 'id_categoria', 'id_categorizacion', 'id_proveedor', 'departamento', 'municipio', 'id_regional', 'id_antiguedad', 
                                                    'id_estado', 'id_fuente', 'fecha_retiro_area']]
        
        
        
    
    except Exception as e:
        fuentes.append(par.nombre_archivo_planta_ajuste+" | "+par.nombre_hoja_red_retiro)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(PrepararTablaPrincipalHechoRetiro.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()


    return df_tabla_hecho_planta

# %%
def PrepararTablaPrincipalHechoDirectos(df_planta_comercial):
    """
    Función que prepara la tabla principal de hechos para la red maestra de planta comercial. Consulta tablas de dominio históricas,
    renombra columnas, realiza limpieza de datos, y consolida los datos en un DataFrame estructurado para su carga en la base de datos.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial
    Retorna:
        df_tabla_hecho_planta: DataFrame de la tabla de hechos lista para carga
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        # Consultar tablas de dominio históricas registradas en base de datos
        diccionario_tablas_dominio_principales = {
            'tb_planta_tipo_documento',
            'tb_planta_genero',
            'tb_planta_cargo',
            'tb_planta_operacion',
            'tb_planta_contrato',
            'tb_planta_tipo_contratacion',
            'tb_planta_contratante',
            'tb_planta_direccion_comercial',
            'tb_planta_segmento',
            'tb_planta_gerencia_jefatura_comercial',
            'tb_planta_grupo_comercial',
            'tb_planta_area',
            'tb_planta_canal',
            'tb_planta_categoria',
            'tb_planta_categorizacion',
            'tb_planta_proveedor',
            'tb_planta_regional',
            'tb_planta_antiguedad',
            'tb_planta_estado',
            'tb_municipio',
            'tb_departamento',
            'tb_planta_especialista',
            'tb_planta_coordinador_tercero',
            'tb_planta_coordinador_directo',
            'tb_planta_jefe',
            'tb_planta_gerente',
            'tb_planta_director',
            'tb_planta_fuente',
        }

        resultados_tablas_dominio = {}
        
        # Consultar todas las tablas de dominio y almacenarlas en resultados_tablas_dominio
        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio[nombre_tabla] = df

        # Obtener las tablas específicas necesarias para mapeos
        df_tipo_documento_actual = resultados_tablas_dominio['tb_planta_tipo_documento']
        df_genero_actual = resultados_tablas_dominio['tb_planta_genero']
        df_cargo_actual = resultados_tablas_dominio['tb_planta_cargo']
        df_operacion_actual = resultados_tablas_dominio['tb_planta_operacion']
        df_contrato_actual = resultados_tablas_dominio['tb_planta_contrato']
        df_tipo_contratacion_actual = resultados_tablas_dominio['tb_planta_tipo_contratacion']
        df_contratante_actual = resultados_tablas_dominio['tb_planta_contratante']
        df_segmento_actual = resultados_tablas_dominio['tb_planta_segmento']
        df_gerencia_jefatura_comercial_actual = resultados_tablas_dominio['tb_planta_gerencia_jefatura_comercial']
        df_grupo_comercial_actual = resultados_tablas_dominio['tb_planta_grupo_comercial']
        df_area_actual = resultados_tablas_dominio['tb_planta_area']
        df_canal_actual = resultados_tablas_dominio['tb_planta_canal']
        df_categoria_actual = resultados_tablas_dominio['tb_planta_categoria']
        df_categorizacion_actual = resultados_tablas_dominio['tb_planta_categorizacion']
        df_proveedor_actual = resultados_tablas_dominio['tb_planta_proveedor']
        df_regional_actual = resultados_tablas_dominio['tb_planta_regional']
        df_antiguedad_actual = resultados_tablas_dominio['tb_planta_antiguedad']
        df_estado_actual = resultados_tablas_dominio['tb_planta_estado']
        df_coordinador_directo_actual = resultados_tablas_dominio['tb_planta_coordinador_directo']
        df_especialista_actual = resultados_tablas_dominio['tb_planta_especialista']
        df_coordinador_tercero_actual = resultados_tablas_dominio['tb_planta_coordinador_tercero']
        df_jefe_actual = resultados_tablas_dominio['tb_planta_jefe']
        df_gerente_actual = resultados_tablas_dominio['tb_planta_gerente']
        df_director_actual = resultados_tablas_dominio['tb_planta_director']
        df_direccion_comercial_actual = resultados_tablas_dominio['tb_planta_direccion_comercial']
        df_fuente = resultados_tablas_dominio['tb_planta_fuente']
        
        # Renombrar columnas de df_planta_comercial según especificaciones
        df_tabla_hecho_planta = df_planta_comercial.rename(columns={
            'CIUDAD INCIDENTE': 'ciudad_incidente',
            'NOMBRE ESPECIALISTA': 'especialista',
            'ALIADO RESIDENCIAL': 'aliado_residencial',
            'FUENTE': 'fuente'
        })

        df_tabla_hecho_planta['genero'] = 'NO APLICA'
        df_tabla_hecho_planta['cargo_actual'] = 'NO APLICA'
        df_tabla_hecho_planta['contrato'] = 'NO APLICA'
        df_tabla_hecho_planta['tipo_contrato'] = 'NO APLICA'
        df_tabla_hecho_planta['contratante'] = 'NO APLICA'
        df_tabla_hecho_planta['canal'] = 'NO APLICA'
        df_tabla_hecho_planta['segmento'] = 'NO APLICA'
        df_tabla_hecho_planta['area'] = 'NO APLICA'
        df_tabla_hecho_planta['grupo'] = 'NO APLICA'
        df_tabla_hecho_planta['categoria'] = 'NO APLICA'
        df_tabla_hecho_planta['categorizacion'] = 'NO APLICA'
        df_tabla_hecho_planta['proveedor'] = 'NO APLICA'
        df_tabla_hecho_planta['regional'] = 'NO APLICA'
        df_tabla_hecho_planta['antiguedad'] = 'NO APLICA'
        df_tabla_hecho_planta['estado'] = 'NO APLICA'
        df_tabla_hecho_planta['gerencia_jefatura'] = 'NO APLICA'
        df_tabla_hecho_planta['jefe'] = 'NO APLICA'
        df_tabla_hecho_planta['coordinador_tercero'] = 'NO APLICA'
        df_tabla_hecho_planta['operacion'] = 'NO APLICA'
        df_tabla_hecho_planta['celular'] = 0
        df_tabla_hecho_planta['correo'] = '0'
        df_tabla_hecho_planta['nacionalidad'] = '0'
        df_tabla_hecho_planta['codigo_ventas_movil'] = '0'
        df_tabla_hecho_planta['direccion_comercial'] = 'NO APLICA'
        df_tabla_hecho_planta['departamento'] = '0'
        df_tabla_hecho_planta['municipio'] = '0'
        df_tabla_hecho_planta['nombre_completo']='NO APLICA'
        df_tabla_hecho_planta['gerente'] = 'NO APLICA'
        df_tabla_hecho_planta['director'] = 'NO APLICA'
        df_tabla_hecho_planta['tipo_documento_hecho'] = 'NO APLICA'
        df_tabla_hecho_planta['identificacion_hecho']= 0
        df_tabla_hecho_planta['fuente']= 'DIRECTOS'
        df_tabla_hecho_planta['fecha_ingreso']= pd.Timestamp('1900-01-01')
        
        # Inicializar id_estado_registro_hecho en 1
        df_tabla_hecho_planta['id_estado_registro_hecho'] = 1

        def asignar_id_especialista_coordinador(nombre):
            try:
                # Verificar si el nombre está vacío o es nulo
                if not nombre or pd.isna(nombre):
                    return 'NO APLICA', 'NO APLICA'

                # Buscar en la tabla de especialista
                existe_en_especialista = not df_especialista_actual[df_especialista_actual['nombre'].str.contains(nombre, case=False, na=False)].empty

                if existe_en_especialista:
                    # Si existe en especialista, asignar NO APLICA a coordinador_directo
                    return nombre, 'NO APLICA'
                else:
                    # Buscar en la tabla de coordinador directo si no está en especialista
                    existe_en_coordinador = not df_coordinador_directo_actual[df_coordinador_directo_actual['nombre'].str.contains(nombre, case=False, na=False)].empty

                    if existe_en_coordinador:
                        # Si existe en coordinador_directo, asignar NO APLICA a especialista
                        return  'NO APLICA', nombre
                    else:
                        # Si no está en ninguna, asignar NO APLICA a ambos
                        return 'NO APLICA', 'NO APLICA'

            except Exception as e:
                # Si ocurre un error, asignar 'NO APLICA' a ambos
                return 'NO APLICA', 'NO APLICA'

        # Aplicar la función y asignar los resultados a las nuevas columnas
        df_tabla_hecho_planta['especialista'], df_tabla_hecho_planta['coordinador_directo'] = zip(*df_tabla_hecho_planta['especialista'].apply(asignar_id_especialista_coordinador))

        # Rellenar los valores nulos en 'especialista' y 'coordinador_directo' con 'NO APLICA'
        df_tabla_hecho_planta['especialista'] = df_tabla_hecho_planta['especialista'].fillna('NO APLICA')
        df_tabla_hecho_planta['coordinador_directo'] = df_tabla_hecho_planta['coordinador_directo'].fillna('NO APLICA')

        # Seleccionar las columnas necesarias y aplicar limpieza
        df_tabla_hecho_planta = df_tabla_hecho_planta[['identificacion_hecho','tipo_documento_hecho','nombre_completo','especialista','jefe','gerente',
                                                    'director','gerencia_jefatura', 'grupo','antiguedad','regional','proveedor','direccion_comercial',
                                                    'genero','cargo_actual','coordinador_tercero','canal','contratante','operacion',
                                                    'area','categoria','categorizacion','estado','codigo_ventas_movil','nacionalidad','correo', 'celular',
                                                    'departamento','municipio', 'aliado_residencial', 'ciudad_incidente','coordinador_directo',
                                                    'tipo_contrato','contrato','fuente','id_estado_registro_hecho','segmento','fecha_ingreso']]

        df_tabla_hecho_planta = limpiezaCamposString(df_tabla_hecho_planta)

        df_tabla_hecho_planta = df_tabla_hecho_planta.dropna(subset=['aliado_residencial','ciudad_incidente', 'especialista'])

        df_tabla_hecho_planta['id'] = [generate_uuid().upper() for _ in range(len(df_tabla_hecho_planta))]
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_documento_actual, left_on='tipo_documento_hecho', right_on= ['tipo_documento'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_genero_actual, left_on='genero', right_on= ['genero'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_cargo_actual, left_on='cargo_actual', right_on= ['cargo_actual'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_operacion_actual, left_on='operacion', right_on= ['operacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_contrato_actual, left_on='contrato', right_on= ['contrato'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_contratacion_actual, left_on='tipo_contrato', right_on= ['tipo_contratacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_contratante_actual, left_on='contratante', right_on= ['contratante'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_segmento_actual, left_on='segmento', right_on= ['segmento'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerencia_jefatura_comercial_actual, left_on='gerencia_jefatura', right_on= ['gerencia_jefatura'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_grupo_comercial_actual, left_on='grupo', right_on= ['grupo'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_area_actual, left_on='area', right_on= ['area'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_canal_actual, left_on='canal', right_on= ['canal'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_categoria_actual, left_on='categoria', right_on= ['categoria'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_categorizacion_actual, left_on='categorizacion', right_on= ['categorizacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_proveedor_actual, left_on='proveedor', right_on= ['proveedor'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_regional_actual, left_on='regional', right_on= ['regional'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_antiguedad_actual, left_on='antiguedad', right_on= ['antiguedad'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_estado_actual, left_on='estado', right_on= ['estado'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_fuente, left_on='fuente', right_on= ['fuente'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        
        if 'direccion_comercial' in df_tabla_hecho_planta.columns:
            df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_direccion_comercial_actual, left_on='direccion_comercial', right_on= ['direccion_comercial'], how='left')
            df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        
            # Verificar las columnas resultantes del merge
            if 'id_direccion_comercial_left' in df_tabla_hecho_planta.columns or 'id_direccion_comercial_right' in df_tabla_hecho_planta.columns:
                df_tabla_hecho_planta['id_direccion_comercial'] = df_tabla_hecho_planta['id_direccion_comercial_right'].combine_first(df_tabla_hecho_planta['id_direccion_comercial_left'])
                df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_direccion_comercial_left', 'id_direccion_comercial_right'], axis=1)
            elif 'id_direccion_comercial' not in df_tabla_hecho_planta.columns:
                df_tabla_hecho_planta['id_direccion_comercial'] = 0

        df_tabla_hecho_planta['coordinador_directo'] = df_tabla_hecho_planta['coordinador_directo'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_coordinador_directo_actual, left_on='coordinador_directo', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','id_tipo_documento_left','id_tipo_documento_right'], axis=1)
        df_tabla_hecho_planta['especialista'] = df_tabla_hecho_planta['especialista'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_especialista_actual, left_on='especialista', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre'], axis=1)
        df_tabla_hecho_planta['coordinador_tercero'] = df_tabla_hecho_planta['coordinador_tercero'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_coordinador_tercero_actual, left_on='coordinador_tercero', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','id_tipo_documento_left','id_tipo_documento_right','identificacion_right'], axis=1)
        df_tabla_hecho_planta['jefe'] = df_tabla_hecho_planta['jefe'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_jefe_actual, left_on='jefe', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','identificacion_right'], axis=1)
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop_duplicates().reset_index(drop=True)
        df_tabla_hecho_planta['gerente'] = df_tabla_hecho_planta['gerente'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerente_actual, left_on='gerente', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','id_tipo_documento_left','id_tipo_documento_right'], axis=1)
        df_tabla_hecho_planta['director'] = df_tabla_hecho_planta['director'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_director_actual, left_on='director', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','identificacion_right'], axis=1)
        

        # Transformaciones finales para la tabla Hecho de planta comercial
        df_tabla_hecho_planta['id_estado_registro_hecho'] = df_tabla_hecho_planta.apply(lambda row: 4 if '(VACANTE)' in row['nombre_completo'] else row['id_estado_registro_hecho'], axis=1)
        df_tabla_hecho_planta['nombre_completo'] = df_tabla_hecho_planta['nombre_completo'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = df_tabla_hecho_planta.rename(columns={'id_tipo_documento_hecho' : 'id_tipo_documento',
                                                                    'identificacion_hecho' : 'identificacion',
                                                                    'id_estado_registro_hecho' : 'id_estado_registro'})

        hoy = datetime.today()
        df_tabla_hecho_planta['fecha_creacion'] = hoy
        df_tabla_hecho_planta['fecha_modificacion'] = hoy
        df_tabla_hecho_planta['id_direccion_comercial'] = 0
        # Seleccionar y ordenar columnas finales
        df_tabla_hecho_planta = df_tabla_hecho_planta[['id', 'id_tipo_documento', 'identificacion','nombre_completo','celular','correo','nacionalidad','fecha_creacion', 'fecha_modificacion', 
                                                    'id_estado_registro', 'codigo_ventas_movil','id_genero', 'id_cargo', 'id_operacion', 'id_contrato','id_tipo_contrato', 'id_contratante', 'id_especialista', 'id_coordinador_tercero', 'id_coordinador_directo',
                                                    'id_jefe', 'id_gerente', 'id_direccion_comercial', 'id_director', 'id_segmento', 'id_gerencia_jefatura', 'id_grupo', 'id_area', 'fecha_ingreso',
                                                    'id_canal', 'id_categoria', 'id_categorizacion', 'id_proveedor', 'departamento', 'municipio', 'id_regional', 'id_antiguedad', 'id_estado',
                                                    'id_fuente','ciudad_incidente', 'aliado_residencial']]
    
        

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta_ajuste+" | "+par.nombre_hoja_red_directos)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(PrepararTablaPrincipalHechoDirectos.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()

    return df_tabla_hecho_planta

# %%
def PrepararTablaPrincipalHechoCavs(df_planta_comercial):
    """
    Función que prepara la tabla principal de hechos para la red maestra de planta comercial enfocada en los datos de CAVS.
    Consulta tablas de dominio históricas, renombra columnas, realiza limpieza de datos y consolida los datos en un DataFrame
    estructurado y consistente para su carga en la base de datos.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial

    Retorna:
        df_tabla_hecho_planta: DataFrame de la tabla de hechos lista para carga

    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        # Consultar tablas de dominio históricas registradas en base de datos
        diccionario_tablas_dominio_principales = {
            'tb_planta_tipo_documento',
            'tb_planta_genero',
            'tb_planta_cargo',
            'tb_planta_operacion',
            'tb_planta_contrato',
            'tb_planta_tipo_contratacion',
            'tb_planta_contratante',
            'tb_planta_direccion_comercial',
            'tb_planta_segmento',
            'tb_planta_gerencia_jefatura_comercial',
            'tb_planta_grupo_comercial',
            'tb_planta_area',
            'tb_planta_canal',
            'tb_planta_categoria',
            'tb_planta_categorizacion',
            'tb_planta_proveedor',
            'tb_planta_regional',
            'tb_planta_antiguedad',
            'tb_planta_estado',
            'tb_municipio',
            'tb_departamento',
            'tb_planta_especialista',
            'tb_planta_coordinador_tercero',
            'tb_planta_coordinador_directo',
            'tb_planta_jefe',
            'tb_planta_gerente',
            'tb_planta_director',
            'tb_planta_fuente',
        }

        resultados_tablas_dominio = {}
        
        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio[nombre_tabla] = df

        df_tipo_documento_actual = resultados_tablas_dominio['tb_planta_tipo_documento']
        df_genero_actual = resultados_tablas_dominio['tb_planta_genero']
        df_cargo_actual = resultados_tablas_dominio['tb_planta_cargo']
        df_operacion_actual = resultados_tablas_dominio['tb_planta_operacion']
        df_contrato_actual = resultados_tablas_dominio['tb_planta_contrato']
        df_tipo_contratacion_actual = resultados_tablas_dominio['tb_planta_tipo_contratacion']
        df_contratante_actual = resultados_tablas_dominio['tb_planta_contratante']
        df_segmento_actual = resultados_tablas_dominio['tb_planta_segmento']
        df_gerencia_jefatura_comercial_actual = resultados_tablas_dominio['tb_planta_gerencia_jefatura_comercial']
        df_grupo_comercial_actual = resultados_tablas_dominio['tb_planta_grupo_comercial']
        df_area_actual = resultados_tablas_dominio['tb_planta_area']
        df_canal_actual = resultados_tablas_dominio['tb_planta_canal']
        df_categoria_actual = resultados_tablas_dominio['tb_planta_categoria']
        df_categorizacion_actual = resultados_tablas_dominio['tb_planta_categorizacion']
        df_proveedor_actual = resultados_tablas_dominio['tb_planta_proveedor']
        df_regional_actual = resultados_tablas_dominio['tb_planta_regional']
        df_antiguedad_actual = resultados_tablas_dominio['tb_planta_antiguedad']
        df_estado_actual = resultados_tablas_dominio['tb_planta_estado']
        df_especialista_actual = resultados_tablas_dominio['tb_planta_especialista']
        df_coordinador_tercero_actual = resultados_tablas_dominio['tb_planta_coordinador_tercero']
        df_coordinador_directo_actual = resultados_tablas_dominio['tb_planta_coordinador_directo']
        df_jefe_actual = resultados_tablas_dominio['tb_planta_jefe']
        df_gerente_actual = resultados_tablas_dominio['tb_planta_gerente']
        df_director_actual = resultados_tablas_dominio['tb_planta_director']
        df_direccion_comercial_actual = resultados_tablas_dominio['tb_planta_direccion_comercial']
        df_fuente = resultados_tablas_dominio['tb_planta_fuente']

        df_direccion_comercial_actual['direccion_comercial'] = df_direccion_comercial_actual['direccion_comercial'].str.strip()
        df_planta_comercial['DIRECCION COMERCIAL'] = df_planta_comercial['DIRECCION COMERCIAL'].str.strip()
        
        df_tabla_hecho_planta = df_planta_comercial.rename(columns={
            'No DOCUMENTO': 'identificacion_hecho',
            'NOMBRE': 'nombre_completo',
            'CAVS': 'cavs',
            'NOMBRE ESPECIALISTA': 'especialista',
            'NOMBRE JEFE': 'jefe',
            'NOMBRE GERENTE': 'gerente',
            'CODIGO PADRE CAV DONDE LABORA': 'codigo_padre_cav',
            'DEALER': 'dealer',
            'CODIGO CVC CONSULTOR ': 'codigo_cvc_consultor',
            'CORREO': 'correo',
            'USUARIO RED':'usuario_red',
            'OBSERVACION': 'observacion',
            'SEGMENTO':'segmento',
            'DIRECCION COMERCIAL':'direccion_comercial',
            'GERENCIA COMERCIAL/ O JEFATURA':'gerencia_jefatura',
            'CARGO ACTUAL':'cargo_actual',
            'CANAL':'canal',
            'CIUDAD':'ciudad',
            'CELULAR':'celular',
            'FUENTE' : 'fuente'
        })
        
        df_tabla_hecho_planta['id_estado_registro_hecho'] = 1
        
        
        NO_APLICA_ID = {
            'especialista': 'NO APLICA', 
            'coordinador_directo': 'NO APLICA'  
        }

        def asignar_id_especialista_coordinador(nombre):
            try:
                # Intentar obtener el ID de especialista
                df_especialista = df_especialista_actual[df_especialista_actual['nombre'].str.contains(nombre, case=False, na=False)]['nombre'].tolist()
                # Intentar obtener el ID de coordinador
                df_coordinador = df_coordinador_directo_actual[df_coordinador_directo_actual['nombre'].str.contains(nombre, case=False, na=False)]['nombre'].tolist()

                # Devolver los ID encontrados o 'NO APLICA' si no se encuentra ninguno
                id_especialista_final = df_especialista[0] if df_especialista else NO_APLICA_ID['especialista']
                id_coordinador_final = df_coordinador[0] if df_coordinador else NO_APLICA_ID['coordinador_directo']
            except IndexError:
                # Si hay un IndexError, asignar 'NO APLICA' a ambos
                id_especialista_final = NO_APLICA_ID['especialista']
                id_coordinador_final = NO_APLICA_ID['coordinador_directo']

            return id_especialista_final, id_coordinador_final

        # Aplicar la función y asignar los resultados a las nuevas columnas
        df_tabla_hecho_planta['especialista'], df_tabla_hecho_planta['coordinador_directo'] = zip(*df_tabla_hecho_planta['especialista'].apply(asignar_id_especialista_coordinador))
        
        df_tabla_hecho_planta = df_tabla_hecho_planta[['identificacion_hecho','nombre_completo','cavs','especialista','observacion',
                                                        'jefe','gerente','codigo_padre_cav','coordinador_directo', 'usuario_red','canal', 'ciudad',
                                                        'segmento','cargo_actual','gerencia_jefatura', 'direccion_comercial', 'celular',
                                                        'dealer','codigo_cvc_consultor','correo','fuente','id_estado_registro_hecho']]
        
        df_tabla_hecho_planta = limpiezaCamposString(df_tabla_hecho_planta)
        
        df_tabla_hecho_planta['genero'] = 'NO APLICA'
        df_tabla_hecho_planta['contrato'] = 'NO APLICA'
        df_tabla_hecho_planta['tipo_contrato'] = 'NO APLICA'
        df_tabla_hecho_planta['contratante'] = 'NO APLICA'
        df_tabla_hecho_planta['area'] = 'NO APLICA'
        df_tabla_hecho_planta['grupo'] = 'NO APLICA'
        df_tabla_hecho_planta['coordinador_directo'] = 'NO APLICA'
        df_tabla_hecho_planta['categoria'] = 'NO APLICA'
        df_tabla_hecho_planta['categorizacion'] = 'NO APLICA'
        df_tabla_hecho_planta['proveedor'] = 'NO APLICA'
        df_tabla_hecho_planta['regional'] = 'NO APLICA'
        df_tabla_hecho_planta['antiguedad'] = 'NO APLICA'
        df_tabla_hecho_planta['grupo'] = 'NO APLICA'
        df_tabla_hecho_planta['estado'] = 'NO APLICA'
        df_tabla_hecho_planta['director'] = 'NO APLICA'
        df_tabla_hecho_planta['coordinador_tercero'] = 'NO APLICA'
        df_tabla_hecho_planta['operacion'] = 'NO APLICA'
        df_tabla_hecho_planta['nacionalidad'] = 'NO APLICA'
        df_tabla_hecho_planta['codigo_ventas_movil'] = 'NO APLICA'
        df_tabla_hecho_planta['departamento'] = 'NO APLICA'
        df_tabla_hecho_planta['fecha_ingreso']= pd.Timestamp('1900-01-01')
        df_tabla_hecho_planta['director'] = 'NO APLICA'
        df_tabla_hecho_planta['coordinador_tercero'] = 'NO APLICA'
        df_tabla_hecho_planta['tipo_documento_hecho'] = 'CC'
        df_tabla_hecho_planta['municipio'] = 'NO APLICA'

        df_tabla_hecho_planta['id'] = [generate_uuid().upper() for _ in range(len(df_tabla_hecho_planta))]
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_documento_actual, left_on='tipo_documento_hecho', right_on= ['tipo_documento'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_genero_actual, left_on='genero', right_on= ['genero'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)

        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_direccion_comercial_actual, left_on='direccion_comercial', right_on= ['direccion_comercial'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        
        df_tabla_hecho_planta['id_direccion_comercial_pr'] = df_tabla_hecho_planta['id_direccion_comercial']
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_cargo_actual, left_on='cargo_actual', right_on= ['cargo_actual'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_operacion_actual, left_on='operacion', right_on= ['operacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_contrato_actual, left_on='contrato', right_on= ['contrato'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_contratacion_actual, left_on='tipo_contrato', right_on= ['tipo_contratacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_contratante_actual, left_on='contratante', right_on= ['contratante'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_segmento_actual, left_on='segmento', right_on= ['segmento'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerencia_jefatura_comercial_actual, left_on='gerencia_jefatura', right_on= ['gerencia_jefatura'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_grupo_comercial_actual, left_on='grupo', right_on= ['grupo'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_area_actual, left_on='area', right_on= ['area'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_canal_actual, left_on='canal', right_on= ['canal'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_categoria_actual, left_on='categoria', right_on= ['categoria'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_categorizacion_actual, left_on='categorizacion', right_on= ['categorizacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_proveedor_actual, left_on='proveedor', right_on= ['proveedor'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_regional_actual, left_on='regional', right_on= ['regional'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_antiguedad_actual, left_on='antiguedad', right_on= ['antiguedad'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_estado_actual, left_on='estado', right_on= ['estado'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_fuente, left_on='fuente', right_on= ['fuente'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta['especialista'] = df_tabla_hecho_planta['especialista'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_especialista_actual, left_on='especialista', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre'], axis=1)
        df_tabla_hecho_planta['coordinador_tercero'] = df_tabla_hecho_planta['coordinador_tercero'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_coordinador_tercero_actual, left_on='coordinador_tercero', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','id_tipo_documento_left','id_tipo_documento_right','identificacion_right'], axis=1)
        df_tabla_hecho_planta['coordinador_directo'] = df_tabla_hecho_planta['coordinador_directo'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_coordinador_directo_actual, left_on='coordinador_directo', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','id_tipo_documento_left','id_tipo_documento_right'], axis=1)
        df_tabla_hecho_planta['gerente'] = df_tabla_hecho_planta['gerente'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerente_actual, left_on='gerente', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre'], axis=1)
        df_tabla_hecho_planta['jefe'] = df_tabla_hecho_planta['jefe'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_jefe_actual, left_on='jefe', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','identificacion_right'], axis=1)
        df_tabla_hecho_planta['director'] = df_tabla_hecho_planta['director'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_director_actual, left_on='director', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','identificacion_right'], axis=1)

        #df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_jefe_actual[['nombre', 'id_jefe']], left_on='jefe', right_on='nombre', how='left')
        #df_tabla_hecho_planta.drop('nombre', axis=1, inplace=True)  

        # Transformaciones para la tabla Hecho de planta comercial
        df_tabla_hecho_planta['id_estado_registro_hecho'] = df_tabla_hecho_planta.apply(lambda row: 4 if '(VACANTE)' in row['nombre_completo'] else row['id_estado_registro_hecho'], axis=1)
        df_tabla_hecho_planta['nombre_completo'] = df_tabla_hecho_planta['nombre_completo'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = df_tabla_hecho_planta.rename(columns={'id_tipo_documento_hecho' : 'id_tipo_documento',
                                                                    'identificacion_hecho' : 'identificacion',
                                                                    'id_estado_registro_hecho' : 'id_estado_registro'})
        
        hoy = datetime.today()
        # Añadir columnas con la fecha de hoy
        df_tabla_hecho_planta['fecha_creacion']  = hoy
        df_tabla_hecho_planta['fecha_modificacion'] = hoy
        df_tabla_hecho_planta['id_tipo_documento'] = 3

        df_tabla_hecho_planta['id_direccion_comercial'] = df_tabla_hecho_planta['id_direccion_comercial_pr']
        df_tabla_hecho_planta = df_tabla_hecho_planta.loc[:, ~df_tabla_hecho_planta.columns.duplicated()]
        df_tabla_hecho_planta['celular'] = df_tabla_hecho_planta['celular'].replace('PENDIENTE', 0)
        
        # Rest of the code
        columnas_necesarias = [
            'id_direccion_comercial', 'id', 'id_tipo_documento', 'identificacion', 'nombre_completo', 'celular', 
            'correo', 'nacionalidad', 'codigo_ventas_movil', 'fecha_creacion', 'fecha_modificacion', 'dealer',
            'id_estado_registro', 'id_genero', 'id_cargo', 'id_operacion', 'observacion', 'id_contrato', 'codigo_cvc_consultor',
            'id_tipo_contrato', 'id_contratante', 'id_especialista', 'id_coordinador_tercero', 'id_coordinador_directo',
            'usuario_red', 'id_jefe', 'id_gerente', 'id_director', 'id_segmento', 'id_gerencia_jefatura', 'ciudad',
            'id_grupo', 'id_area', 'fecha_ingreso', 'id_canal', 'id_categoria', 'id_categorizacion', 'cavs', 'codigo_padre_cav',
            'id_proveedor', 'departamento', 'municipio', 'id_regional', 'id_antiguedad', 'id_estado', 'id_fuente'
        ]

        # Filtrar el DataFrame por las columnas necesarias, omitiendo las que no existan
        columnas_presentes = [col for col in columnas_necesarias if col in df_tabla_hecho_planta.columns]
        df_tabla_hecho_planta = df_tabla_hecho_planta[columnas_presentes]

        
    

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta+" | "+par.nombre_hoja_red_cavs)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(PrepararTablaPrincipalHechoCavs.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()

    return df_tabla_hecho_planta

# %%
def PrepararTablaPrincipalHechoTmk(df_planta_comercial):
    """
    Función que prepara la tabla principal de hechos para la red maestra de planta comercial. Consulta tablas de dominio históricas,
    renombra columnas, realiza limpieza de datos, y consolida los datos en un DataFrame estructurado para su carga en la base de datos.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial
    Retorna:
        df_tabla_hecho_planta: DataFrame de la tabla de hechos lista para carga
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        # Consultar tablas de dominio históricas registradas en base de datos
        diccionario_tablas_dominio_principales = {
            'tb_planta_tipo_documento',
            'tb_planta_genero',
            'tb_planta_cargo',
            'tb_planta_operacion',
            'tb_planta_contrato',
            'tb_planta_tipo_contratacion',
            'tb_planta_contratante',
            'tb_planta_direccion_comercial',
            'tb_planta_segmento',
            'tb_planta_gerencia_jefatura_comercial',
            'tb_planta_grupo_comercial',
            'tb_planta_area',
            'tb_planta_canal',
            'tb_planta_categoria',
            'tb_planta_categorizacion',
            'tb_planta_proveedor',
            'tb_planta_regional',
            'tb_planta_antiguedad',
            'tb_planta_estado',
            'tb_municipio',
            'tb_departamento',
            'tb_planta_especialista',
            'tb_planta_coordinador_tercero',
            'tb_planta_coordinador_directo',
            'tb_planta_jefe',
            'tb_planta_gerente',
            'tb_planta_director',
            'tb_planta_fuente',
        }

        resultados_tablas_dominio = {}
        
        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio[nombre_tabla] = df

        df_tipo_documento_actual = resultados_tablas_dominio['tb_planta_tipo_documento']
        df_genero_actual = resultados_tablas_dominio['tb_planta_genero']
        df_cargo_actual = resultados_tablas_dominio['tb_planta_cargo']
        df_operacion_actual = resultados_tablas_dominio['tb_planta_operacion']
        df_contrato_actual = resultados_tablas_dominio['tb_planta_contrato']
        df_tipo_contratacion_actual = resultados_tablas_dominio['tb_planta_tipo_contratacion']
        df_contratante_actual = resultados_tablas_dominio['tb_planta_contratante']
        df_segmento_actual = resultados_tablas_dominio['tb_planta_segmento']
        df_gerencia_jefatura_comercial_actual = resultados_tablas_dominio['tb_planta_gerencia_jefatura_comercial']
        df_grupo_comercial_actual = resultados_tablas_dominio['tb_planta_grupo_comercial']
        df_area_actual = resultados_tablas_dominio['tb_planta_area']
        df_canal_actual = resultados_tablas_dominio['tb_planta_canal']
        df_categoria_actual = resultados_tablas_dominio['tb_planta_categoria']
        df_categorizacion_actual = resultados_tablas_dominio['tb_planta_categorizacion']
        df_proveedor_actual = resultados_tablas_dominio['tb_planta_proveedor']
        df_regional_actual = resultados_tablas_dominio['tb_planta_regional']
        df_antiguedad_actual = resultados_tablas_dominio['tb_planta_antiguedad']
        df_estado_actual = resultados_tablas_dominio['tb_planta_estado']
        df_especialista_actual = resultados_tablas_dominio['tb_planta_especialista']
        df_coordinador_tercero_actual = resultados_tablas_dominio['tb_planta_coordinador_tercero']
        df_coordinador_directo_actual = resultados_tablas_dominio['tb_planta_coordinador_directo']
        df_jefe_actual = resultados_tablas_dominio['tb_planta_jefe']
        df_gerente_actual = resultados_tablas_dominio['tb_planta_gerente']
        df_director_actual = resultados_tablas_dominio['tb_planta_director']
        df_direccion_comercial_actual = resultados_tablas_dominio['tb_planta_direccion_comercial']
        df_fuente = resultados_tablas_dominio['tb_planta_fuente']

        df_direccion_comercial_actual['direccion_comercial'] = df_direccion_comercial_actual['direccion_comercial'].str.strip()
        df_planta_comercial['DIRECCION COMERCIAL'] = df_planta_comercial['DIRECCION COMERCIAL'].str.strip()
        
        df_tabla_hecho_planta = df_planta_comercial.rename(columns={
            'Codigo': 'codigo',
            'Dealer': 'dealer',
            'NOMBRE DEL DEALER': 'nombre_completo',
            'ALIADO': 'aliado',
            'TIPO DE CVC': 'tipo_cvc',
            'OPERACIÓN': 'operacion',
            'REGION': 'region',
            'GERENCIA': 'gerencia_jefatura',
            'NOMBRE DE GERENTE': 'gerente',
            'JEFE': 'jefe',
            'ESPECIALISTA': 'especialista',
            'DIRECCION COMERCIAL':'direccion_comercial',
            'NOMBRE DIRECTOR':'director',
            'OBSERVACION': 'observacion',
            'FUENTE' : 'fuente'
        })
    
        df_tabla_hecho_planta['tipo_documento_hecho'] = 'NO APLICA'
        df_tabla_hecho_planta['genero'] = 'NO APLICA'
        df_tabla_hecho_planta['cargo_actual'] = 'NO APLICA'
        df_tabla_hecho_planta['contrato'] = 'NO APLICA'
        df_tabla_hecho_planta['tipo_contrato'] = 'NO APLICA'
        df_tabla_hecho_planta['contratante'] = 'NO APLICA'
        df_tabla_hecho_planta['canal'] = 'NO APLICA'
        df_tabla_hecho_planta['segmento'] = 'NO APLICA'
        df_tabla_hecho_planta['area'] = 'NO APLICA'
        df_tabla_hecho_planta['grupo'] = 'NO APLICA'
        df_tabla_hecho_planta['categoria'] = 'NO APLICA'
        df_tabla_hecho_planta['categorizacion'] = 'NO APLICA'
        df_tabla_hecho_planta['proveedor'] = 'NO APLICA'
        df_tabla_hecho_planta['regional'] = 'NO APLICA'
        df_tabla_hecho_planta['antiguedad'] = 'NO APLICA'
        df_tabla_hecho_planta['estado'] = 'NO APLICA'
        df_tabla_hecho_planta['coordinador_tercero'] = 'NO APLICA'
        df_tabla_hecho_planta['coordinador_directo'] = 'NO APLICA'
        df_tabla_hecho_planta['celular'] = 0
        df_tabla_hecho_planta['correo'] = '0'
        df_tabla_hecho_planta['nacionalidad'] = '0'
        df_tabla_hecho_planta['codigo_ventas_movil'] = '0'
        df_tabla_hecho_planta['departamento'] = '0'
        df_tabla_hecho_planta['municipio'] = '0'
        df_tabla_hecho_planta['tipo_documento_hecho'] = 'NO APLICA'
        df_tabla_hecho_planta['id_estado_registro_hecho'] = 1
        df_tabla_hecho_planta['identificacion_hecho']=0
        df_tabla_hecho_planta['fecha_ingreso']= pd.Timestamp('1900-01-01')

        df_tabla_hecho_planta = df_tabla_hecho_planta[['identificacion_hecho','tipo_documento_hecho','nombre_completo','especialista','jefe','gerente',
                                                    'director','gerencia_jefatura', 'grupo','coordinador_directo','antiguedad','regional','proveedor',
                                                    'direccion_comercial','genero','cargo_actual','coordinador_tercero','canal','contratante','operacion',
                                                    'area','categoria','categorizacion','estado','codigo_ventas_movil','nacionalidad','correo', 'celular',
                                                    'departamento','municipio','observacion','tipo_contrato','contrato','dealer','fuente','id_estado_registro_hecho','segmento','fecha_ingreso']]
        
        df_tabla_hecho_planta = limpiezaCamposString(df_tabla_hecho_planta)
    

        df_tabla_hecho_planta['id'] = [generate_uuid().upper() for _ in range(len(df_tabla_hecho_planta))]
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_documento_actual, left_on='tipo_documento_hecho', right_on= ['tipo_documento'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_genero_actual, left_on='genero', right_on= ['genero'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_cargo_actual, left_on='cargo_actual', right_on= ['cargo_actual'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_operacion_actual, left_on='operacion', right_on= ['operacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_contrato_actual, left_on='contrato', right_on= ['contrato'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_contratacion_actual, left_on='tipo_contrato', right_on= ['tipo_contratacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_contratante_actual, left_on='contratante', right_on= ['contratante'], how='left')
        
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_direccion_comercial_actual, left_on='direccion_comercial', right_on= ['direccion_comercial'], how='left') 
        df_tabla_hecho_planta['id_direccion_comercial_pr'] = df_tabla_hecho_planta['id_direccion_comercial']
        

        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_segmento_actual, left_on='segmento', right_on= ['segmento'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerencia_jefatura_comercial_actual, left_on='gerencia_jefatura', right_on= ['gerencia_jefatura'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_grupo_comercial_actual, left_on='grupo', right_on= ['grupo'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_area_actual, left_on='area', right_on= ['area'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_canal_actual, left_on='canal', right_on= ['canal'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_categoria_actual, left_on='categoria', right_on= ['categoria'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_categorizacion_actual, left_on='categorizacion', right_on= ['categorizacion'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_proveedor_actual, left_on='proveedor', right_on= ['proveedor'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_regional_actual, left_on='regional', right_on= ['regional'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_antiguedad_actual, left_on='antiguedad', right_on= ['antiguedad'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_estado_actual, left_on='estado', right_on= ['estado'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_fuente, left_on='fuente', right_on= ['fuente'], how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta['especialista'] = df_tabla_hecho_planta['especialista'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_especialista_actual, left_on='especialista', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre'], axis=1)
        df_tabla_hecho_planta['coordinador_tercero'] = df_tabla_hecho_planta['coordinador_tercero'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_coordinador_tercero_actual, left_on='coordinador_tercero', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','id_tipo_documento_left','id_tipo_documento_right','identificacion_right'], axis=1)
        df_tabla_hecho_planta['coordinador_directo'] = df_tabla_hecho_planta['coordinador_directo'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_coordinador_directo_actual, left_on='coordinador_directo', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','id_tipo_documento_left','id_tipo_documento_right'], axis=1)
        df_tabla_hecho_planta['jefe'] = df_tabla_hecho_planta['jefe'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_jefe_actual, left_on='jefe', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','identificacion_right'], axis=1)
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop_duplicates().reset_index(drop=True)
        df_tabla_hecho_planta['gerente'] = df_tabla_hecho_planta['gerente'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerente_actual, left_on='gerente', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','id_tipo_documento_left','id_tipo_documento_right'], axis=1)
        df_tabla_hecho_planta['director'] = df_tabla_hecho_planta['director'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_director_actual, left_on='director', right_on= ['nombre'], how='left',suffixes=('_left', '_right'))
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion','nombre','identificacion_left','identificacion_right'], axis=1)
        


        
        #Traformaciones para la tabla Hecho de planta comercial

        df_tabla_hecho_planta['id_estado_registro_hecho'] = df_tabla_hecho_planta.apply(lambda row: 4 if '(VACANTE)' in row['nombre_completo'] else row['id_estado_registro_hecho'], axis=1)
        df_tabla_hecho_planta['nombre_completo'] = df_tabla_hecho_planta['nombre_completo'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = df_tabla_hecho_planta.rename(columns={'id_tipo_documento_hecho' : 'id_tipo_documento',
                                                                    'identificacion_hecho' : 'identificacion',
                                                                    'id_estado_registro_hecho' : 'id_estado_registro'})
        
        hoy = datetime.today()
        df_tabla_hecho_planta['id_tipo_documento'].fillna(3, inplace=True)
        df_tabla_hecho_planta['id_direccion_comercial'] = df_tabla_hecho_planta['id_direccion_comercial_pr']
        
        #print(df_tabla_hecho_planta.dtypes)
        # Añadir columnas con la fecha de hoy
        df_tabla_hecho_planta['fecha_creacion']  = hoy
        df_tabla_hecho_planta['fecha_modificacion'] = hoy


        df_tabla_hecho_planta = df_tabla_hecho_planta[['id', 'id_tipo_documento', 'identificacion', 'nombre_completo', 'celular', 'correo', 'nacionalidad', 'codigo_ventas_movil', 
                                                    'fecha_ingreso', 'fecha_creacion', 'fecha_modificacion', 'dealer', 'id_estado_registro', 'id_genero', 'id_cargo', 'id_operacion', 
                                                    'id_contrato', 'id_tipo_contrato', 'id_contratante', 'id_especialista', 'id_coordinador_tercero', 'id_coordinador_directo','observacion',
                                                    'id_jefe', 'id_gerente', 'id_direccion_comercial', 'id_director', 'id_segmento', 'id_gerencia_jefatura', 'id_grupo', 'id_area', 
                                                    'id_canal', 'id_categoria', 'id_categorizacion', 'id_proveedor', 'departamento', 'municipio', 'id_regional', 'id_antiguedad', 
                                                    'id_estado', 'id_fuente']]
        
        
        
    
    except Exception as e:
        fuentes.append(par.nombre_archivo_planta+" | "+par.nombre_hoja_red_tmk)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(PrepararTablaPrincipalHechoTmk.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()

    return df_tabla_hecho_planta

# %%
def preparacionCargueTablasDominiointeligencia_comercial(df_planta_comercial):
    """
    Función que se encarga de crear las tablas de dominio de la base de datos a partir del archivo
    planta comercial, estas tablas son creadas a partir de información nueva que no existe actualmente
    en los registros de las tablas involucradas 

    Argumentos:
        df_planta_comercial: base de excel de planta comercial
    Retorna: 
        None
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        # Construir tablas de dominio de base planta comercial
        dataframes_resultantes = tablasDominio(df_planta_comercial, fecha_inicio_date)

        df_tipo_documento = dataframes_resultantes.get('TIPO DOCUMENTO')
        df_cargo = dataframes_resultantes.get('CARGO ACTUAL')
        df_tipo_contratacion = dataframes_resultantes.get('TIPO DE CONTRATACION')
        df_gerencia_jefatura_comercial = dataframes_resultantes.get('GERENCIA COMERCIAL/ O JEFATURA')
        df_proveedor = dataframes_resultantes.get('PROVEEDOR')
        df_direccion_comercial = dataframes_resultantes.get('DIRECCION COMERCIAL')
        df_segmento = dataframes_resultantes.get('SEGMENTO')
        df_canal = dataframes_resultantes.get('CANAL')
        df_jefe = dataframes_resultantes.get('NOMBRE JEFE')
        df_jefe_documento = dataframes_resultantes.get('DOCUMENTO JEFE')
        
        df_tipo_documento = df_tipo_documento.rename(columns={'TIPO DOCUMENTO': 'tipo_documento'})
        df_cargo = df_cargo.rename(columns={'CARGO ACTUAL': 'cargo_actual'})
        df_tipo_contratacion = df_tipo_contratacion.rename(columns={'TIPO DE CONTRATACION': 'tipo_contratacion'})
        df_gerencia_jefatura_comercial = df_gerencia_jefatura_comercial.rename(columns={'GERENCIA COMERCIAL/ O JEFATURA': 'gerencia_jefatura'})
        df_proveedor = df_proveedor.rename(columns={'PROVEEDOR': 'proveedor'})
        df_direccion_comercial = df_direccion_comercial.rename(columns={'DIRECCION COMERCIAL': 'direccion_comercial'})
        df_segmento = df_segmento.rename(columns={'SEGMENTO': 'segmento'})
        df_canal = df_canal.rename(columns={'CANAL': 'canal'})

        # Consultar tablas de dominio históricas registradas en base de datos
        diccionario_tablas_dominio_principales = {
            'tb_planta_inteligencia_comercial_tipo_documento',
            'tb_planta_inteligencia_comercial_cargo_actual',
            'tb_planta_inteligencia_comercial_tipo_contratacion',
            'tb_planta_inteligencia_comercial_gerencia_jefatura_comercial',
            'tb_planta_inteligencia_comercial_proveedor',
            'tb_planta_inteligencia_comercial_segmento',
            'tb_planta_inteligencia_comercial_direccion_comercial',
        }

        resultados_tablas_dominio_principales = {}
        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_principales[nombre_tabla] = df

        df_tipo_documento_hist = resultados_tablas_dominio_principales['tb_planta_inteligencia_comercial_tipo_documento'][['tipo_documento']]
        df_cargo_hist = resultados_tablas_dominio_principales['tb_planta_inteligencia_comercial_cargo_actual'][['cargo_actual']]
        df_tipo_contratacion_hist = resultados_tablas_dominio_principales['tb_planta_inteligencia_comercial_tipo_contratacion'][['tipo_contratacion']]
        df_direccion_comercial_hist = resultados_tablas_dominio_principales['tb_planta_inteligencia_comercial_direccion_comercial'][['direccion_comercial']]
        df_segmento_hist = resultados_tablas_dominio_principales['tb_planta_inteligencia_comercial_segmento'][['segmento']]
        df_gerencia_jefatura_comercial_hist = resultados_tablas_dominio_principales['tb_planta_inteligencia_comercial_gerencia_jefatura_comercial'][['gerencia_jefatura']]
        df_proveedor_hist = resultados_tablas_dominio_principales['tb_planta_inteligencia_comercial_proveedor'][['proveedor']]

        # Identificar los registros que son nuevos y no existen actualmente en la base de datos para las tablas de dominio principales
        def identificar_nuevos(df_actual, df_hist, col):
            return pd.merge(df_actual, df_hist[[col]], on=col, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

        df_tipo_documento_nuevo = identificar_nuevos(df_tipo_documento, df_tipo_documento_hist, 'tipo_documento')
        df_cargo_nuevo = identificar_nuevos(df_cargo, df_cargo_hist, 'cargo_actual')
        df_tipo_contratacion_nuevo = identificar_nuevos(df_tipo_contratacion, df_tipo_contratacion_hist, 'tipo_contratacion')
        df_direccion_comercial_nuevo = identificar_nuevos(df_direccion_comercial, df_direccion_comercial_hist, 'direccion_comercial')
        df_segmento_nuevo = identificar_nuevos(df_segmento, df_segmento_hist, 'segmento')
        df_gerencia_jefatura_comercial_nuevo = identificar_nuevos(df_gerencia_jefatura_comercial, df_gerencia_jefatura_comercial_hist, 'gerencia_jefatura')
        df_proveedor_nuevo = identificar_nuevos(df_proveedor, df_proveedor_hist, 'proveedor')

        diccionario_tablas_dominio_principal_cargue = {
            'tb_planta_inteligencia_comercial_tipo_documento': df_tipo_documento_nuevo,
            'tb_planta_inteligencia_comercial_cargo_actual': df_cargo_nuevo,
            'tb_planta_inteligencia_comercial_tipo_contratacion': df_tipo_contratacion_nuevo,
            'tb_planta_inteligencia_comercial_direccion_comercial': df_direccion_comercial_nuevo,
            'tb_planta_inteligencia_comercial_segmento': df_segmento_nuevo,
            'tb_planta_inteligencia_comercial_gerencia_jefatura_comercial': df_gerencia_jefatura_comercial_nuevo,
            'tb_planta_inteligencia_comercial_proveedor': df_proveedor_nuevo
        }

        for nombre_tabla, df_final in diccionario_tablas_dominio_principal_cargue.items():
            if not df_final.empty:
                df_final['id_estado_registro'] = df_final['id_estado_registro'].astype(int)
                cargueDatosBD(nombre_tabla, df_final)

        # Re-consultar tablas de dominio cargadas y actualizadas
        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_principales[nombre_tabla] = df

        df_tipo_documento_actualizada = resultados_tablas_dominio_principales['tb_planta_inteligencia_comercial_tipo_documento']
        df_direccion_comercial_actualizada = resultados_tablas_dominio_principales['tb_planta_inteligencia_comercial_direccion_comercial']

        # Consultar histórico de tablas de dominio secundarias de cargos asociados
        diccionario_tablas_dominio_secundarias = {
            'tb_planta_inteligencia_comercial_jefe'
        }

        resultados_tablas_dominio_secundarias = {}
        for nombre_tabla in diccionario_tablas_dominio_secundarias:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio_secundarias[nombre_tabla] = df

        df_jefe_hist = resultados_tablas_dominio_secundarias['tb_planta_inteligencia_comercial_jefe'][['nombre']]

        df_jefe = df_jefe.rename(columns={'NOMBRE JEFE': 'nombre'})
        df_jefe = pd.merge(df_jefe, df_planta_comercial, left_on='nombre', right_on='NOMBRE', how='left')
        df_jefe = df_jefe[['TIPO DOCUMENTO', 'No DOCUMENTO', 'nombre', 'fecha_creacion', 'fecha_modificacion', 'id_estado_registro']]
        df_jefe = df_jefe.drop_duplicates(subset=['nombre']).reset_index(drop=True)
        df_jefe_null_identificacion = df_jefe[df_jefe['No DOCUMENTO'].isnull()]
        df_jefe_null_identificacion = df_jefe_null_identificacion.rename(columns={'No DOCUMENTO': 'identificacion'})
        df_jefe = df_jefe[df_jefe['No DOCUMENTO'].notnull()]
        df_jefe = df_jefe.rename(columns={'TIPO DOCUMENTO': 'tipo_documento', 'No DOCUMENTO': 'identificacion'})
        df_jefe['identificacion'] = df_jefe['identificacion'].apply(lambda x: int(x))
        df_jefe = df_jefe.drop_duplicates()
        df_jefe['id_jefe'] = df_jefe.apply(lambda row: generate_uuid().upper(), axis=1)
        df_jefe = df_jefe[['id_jefe', 'tipo_documento', 'identificacion', 'nombre', 'fecha_creacion', 'fecha_modificacion', 'id_estado_registro']]

        # Asignar identificación válida para "MARIA LUISA ESCOLAR SUNDHEIM"
        df_jefe_null_identificacion = df_jefe_null_identificacion[['nombre', 'fecha_creacion', 'fecha_modificacion', 'id_estado_registro']]
        df_jefe_null_identificacion = pd.merge(df_jefe_null_identificacion, df_planta_comercial, left_on='nombre', right_on='NOMBRE JEFE', how='left')
        df_jefe_null_identificacion = df_jefe_null_identificacion.rename(columns={'DOCUMENTO JEFE': 'identificacion'})
        df_jefe_null_identificacion['tipo_documento'] = 'CC'
        df_jefe_null_identificacion['identificacion'] = df_jefe_null_identificacion['identificacion'].fillna(1234)
        df_jefe_null_identificacion = df_jefe_null_identificacion.drop_duplicates().head(1)
        df_jefe_null_identificacion['id_jefe'] = df_jefe_null_identificacion.apply(lambda row: generate_uuid().upper(), axis=1)
        df_jefe_null_identificacion = df_jefe_null_identificacion[['id_jefe', 'tipo_documento', 'identificacion', 'nombre', 'fecha_creacion', 'fecha_modificacion', 'id_estado_registro']]

        df_jefe_final = pd.concat([df_jefe, df_jefe_null_identificacion])
        df_jefe_final = pd.merge(df_jefe_final, df_tipo_documento_actualizada, left_on='tipo_documento', right_on='tipo_documento', how='left', indicator=True, suffixes=('_izquierda', '_derecha'))
        df_jefe_final = df_jefe_final.drop(['fecha_creacion_derecha', 'fecha_modificacion_derecha', 'id_estado_registro_derecha', '_merge'], axis=1)
        df_jefe_final.columns = [col.replace('_izquierda', '') for col in df_jefe_final.columns]
        df_jefe_final = df_jefe_final[['id_jefe', 'id_tipo_documento', 'identificacion', 'nombre', 'fecha_creacion', 'fecha_modificacion', 'id_estado_registro']]
        df_jefe_final['id_estado_registro'] = 5
        df_jefe_final['nombre'] = df_jefe_final['nombre'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_jefe_final = df_jefe_final.sort_values(by='id_estado_registro', ascending=True)
        df_jefe_final = df_jefe_final.drop_duplicates(subset=['nombre']).reset_index(drop=True)

        # Identificar los registros que son nuevos y no existen actualmente en la base de datos para las tablas de dominio secundarias
        df_jefe_nuevo = pd.merge(df_jefe_final, df_jefe_hist, left_on='nombre', right_on='nombre', how='outer', indicator=True, suffixes=('_actual', '_historico')).query('_merge == "left_only"')
        df_jefe_nuevo = df_jefe_nuevo.drop('_merge', axis=1)

        diccionario_tablas_dominio_secundarias_cargue = {
            'tb_planta_inteligencia_comercial_jefe': df_jefe_nuevo
        }

        for nombre_tabla, df_final in diccionario_tablas_dominio_secundarias_cargue.items():
            if not df_final.empty:
                df_final['id_estado_registro'] = df_final['id_estado_registro'].astype(int)
                cargueDatosBD(nombre_tabla, df_final)

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(preparacionCargueTablasDominiointeligencia_comercial.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def PrepararTablaPrincipalHechointeligencia_comercial(df_planta_comercial):
    """
    Función que prepara la tabla principal de hechos para la red maestra de planta comercial. Consulta tablas de dominio históricas,
    renombra columnas, realiza limpieza de datos, y consolida los datos en un DataFrame estructurado para su carga en la base de datos.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial
    Retorna:
        df_tabla_hecho_planta: DataFrame de la tabla de hechos lista para carga
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        diccionario_tablas_dominio_principales = {
            'tb_planta_inteligencia_comercial_tipo_documento',
            'tb_planta_inteligencia_comercial_cargo_actual',
            'tb_planta_inteligencia_comercial_tipo_contratacion',
            'tb_planta_inteligencia_comercial_gerencia_jefatura_comercial',
            'tb_planta_inteligencia_comercial_proveedor',
            'tb_planta_inteligencia_comercial_segmento',
            'tb_planta_inteligencia_comercial_direccion_comercial',
            'tb_planta_gerencia_jefatura_comercial',
            'tb_planta_inteligencia_comercial_jefe',
            'tb_planta_fuente',
        }

        resultados_tablas_dominio = {}
        
        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio[nombre_tabla] = df

        df_tipo_documento_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_tipo_documento']
        df_cargo_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_cargo_actual']
        df_tipo_contratacion_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_tipo_contratacion']
        df_segmento_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_segmento']
        df_gerencia_jefatura_comercial_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_gerencia_jefatura_comercial']
        df_proveedor_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_proveedor']
        df_jefe_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_jefe']
        df_direccion_comercial_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_direccion_comercial']
        df_fuente = resultados_tablas_dominio['tb_planta_fuente']

        df_tabla_hecho_planta = df_planta_comercial.rename(columns={
            'TIPO DOCUMENTO': 'tipo_documento_hecho',
            'No DOCUMENTO': 'identificacion_hecho',
            'NOMBRE': 'nombre_completo',
            'CELULAR': 'celular',
            'CORREO': 'correo',
            'CARGO ACTUAL': 'cargo_actual',
            'TIPO DE CONTRATACION': 'tipo_contrato',
            'GERENCIA COMERCIAL/ O JEFATURA': 'gerencia_jefatura',
            'NOMBRE JEFE': 'jefe',
            'PROVEEDOR': 'proveedor',
            'CIUDAD': 'ciudad',
            'FECHA INGRESO AREA': 'fecha_ingreso',
            'SEGMENTO': 'segmento',
            'DIRECCION COMERCIAL': 'direccion_comercial',
            'CANAL': 'canal',
            'FUENTE' : 'fuente'
        })

        df_tabla_hecho_planta['fecha_retiro_area'] = pd.Timestamp('1900-01-01')
        df_tabla_hecho_planta['id_estado_registro_hecho'] = 1

        df_tabla_hecho_planta = df_tabla_hecho_planta[['tipo_documento_hecho', 'identificacion_hecho', 'nombre_completo', 'celular', 
                                                       'correo', 'cargo_actual', 'tipo_contrato', 'gerencia_jefatura', 'jefe',
                                                       'proveedor', 'ciudad', 'fecha_ingreso', 'segmento', 
                                                       'direccion_comercial', 'canal', 'fuente']]
        
        df_tabla_hecho_planta = limpiezaCamposString(df_tabla_hecho_planta)
        
        df_tabla_hecho_planta['id'] = [generate_uuid().upper() for _ in range(len(df_tabla_hecho_planta))]
        
        # Ensure unique IDs
        while df_tabla_hecho_planta['id'].duplicated().any():
            df_tabla_hecho_planta.loc[df_tabla_hecho_planta['id'].duplicated(), 'id'] = df_tabla_hecho_planta['id'].apply(lambda x: generate_uuid().upper())
        
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_documento_actual, left_on='tipo_documento_hecho', right_on='tipo_documento', how='left')
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_cargo_actual, left_on='cargo_actual', right_on='cargo_actual', how='left')
        
        columns_to_drop = ['id_estado_registro', 'fecha_creacion', 'fecha_modificacion']
        columns_to_drop = [col for col in columns_to_drop if col in df_tabla_hecho_planta.columns]
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(columns_to_drop, axis=1)

        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_contratacion_actual, left_on='tipo_contrato', right_on='tipo_contratacion', how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_segmento_actual, left_on='segmento', right_on='segmento', how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerencia_jefatura_comercial_actual, left_on='gerencia_jefatura', right_on='gerencia_jefatura', how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_proveedor_actual, left_on='proveedor', right_on='proveedor', how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_fuente, left_on='fuente', right_on='fuente', how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_direccion_comercial_actual, left_on='direccion_comercial', right_on='direccion_comercial', how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        
        df_tabla_hecho_planta['id_direccion_comercial_pr'] = df_tabla_hecho_planta['id_direccion_comercial']
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_jefe_actual, left_on='jefe', right_on='nombre', how='left', suffixes=('_left', '_right'))
        
        columns_to_drop = ['id_estado_registro', 'fecha_creacion', 'fecha_modificacion', 'nombre', 'identificacion_left', 'identificacion_right']
        columns_to_drop = [col for col in columns_to_drop if col in df_tabla_hecho_planta.columns]
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(columns_to_drop, axis=1)
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop_duplicates().reset_index(drop=True)
        df_tabla_hecho_planta = df_tabla_hecho_planta.rename(columns={'id_tipo_documento_hecho': 'id_tipo_documento',
                                                                      'identificacion_hecho': 'identificacion',
                                                                      'id_estado_registro_hecho': 'id_estado_registro'})
        
        hoy = datetime.today()
    
        df_tabla_hecho_planta['fecha_creacion'] = hoy
        df_tabla_hecho_planta['fecha_modificacion'] = hoy
        df_tabla_hecho_planta['id_tipo_documento'] = df_tabla_hecho_planta['id_tipo_documento_right']
        df_tabla_hecho_planta['id_estado_registro'] = df_tabla_hecho_planta['id_estado_registro_y']
        df_tabla_hecho_planta['id_direccion_comercial'] = df_tabla_hecho_planta['id_direccion_comercial_pr']
        
        df_tabla_hecho_planta['id_tipo_documento'] = df_tabla_hecho_planta['id_tipo_documento'].fillna(1).astype(int)
        df_tabla_hecho_planta = df_tabla_hecho_planta.loc[:, ~df_tabla_hecho_planta.columns.duplicated()]

        df_tabla_hecho_planta = df_tabla_hecho_planta.drop_duplicates(subset=['identificacion', 'nombre_completo']) 
        
        
        df_tabla_hecho_planta = df_tabla_hecho_planta[[
            'id', 'id_tipo_documento', 'identificacion', 'nombre_completo', 'celular', 
            'correo', 'fecha_ingreso', 'fecha_creacion', 'fecha_modificacion', 
            'id_estado_registro', 'id_cargo', 'id_tipo_contrato', 'id_jefe',
            'id_direccion_comercial', 'id_segmento', 'id_gerencia_jefatura', 'ciudad',
            'id_proveedor', 'id_fuente'
        ]]

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta + " | " + par.nombre_hoja_red_inteligencia_comercial)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(PrepararTablaPrincipalHechointeligencia_comercial.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()

    return df_tabla_hecho_planta

# %%
def PrepararTablaPrincipalHechointeligencia_comercial_Retiro(df_planta_comercial):
    """
    Función que prepara la tabla principal de hechos para la red maestra de planta comercial. Consulta tablas de dominio históricas,
    renombra columnas, realiza limpieza de datos, y consolida los datos en un DataFrame estructurado para su carga en la base de datos.

    Argumentos:
        df_planta_comercial: DataFrame de planta comercial
    Retorna:
        df_tabla_hecho_planta: DataFrame de la tabla de hechos lista para carga
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """

    try:
        diccionario_tablas_dominio_principales = {
            'tb_planta_inteligencia_comercial_tipo_documento',
            'tb_planta_inteligencia_comercial_cargo_actual',
            'tb_planta_inteligencia_comercial_tipo_contratacion',
            'tb_planta_inteligencia_comercial_gerencia_jefatura_comercial',
            'tb_planta_inteligencia_comercial_proveedor',
            'tb_planta_inteligencia_comercial_segmento',
            'tb_planta_inteligencia_comercial_direccion_comercial',
            'tb_planta_gerencia_jefatura_comercial',
            'tb_planta_inteligencia_comercial_jefe',
            'tb_planta_fuente',
        }

        resultados_tablas_dominio = {}
        
        for nombre_tabla in diccionario_tablas_dominio_principales:
            df = consultarTablasPlantaComercialHistorico(nombre_tabla)
            resultados_tablas_dominio[nombre_tabla] = df

        df_tipo_documento_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_tipo_documento']
        df_cargo_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_cargo_actual']
        df_tipo_contratacion_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_tipo_contratacion']
        df_segmento_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_segmento']
        df_gerencia_jefatura_comercial_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_gerencia_jefatura_comercial']
        df_proveedor_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_proveedor']
        df_jefe_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_jefe']
        df_direccion_comercial_actual = resultados_tablas_dominio['tb_planta_inteligencia_comercial_direccion_comercial']
        df_fuente = resultados_tablas_dominio['tb_planta_fuente']

        df_tabla_hecho_planta = df_planta_comercial.rename(columns={
            'TIPO DOCUMENTO': 'tipo_documento_hecho',
            'No DOCUMENTO': 'identificacion_hecho',
            'NOMBRE': 'nombre_completo',
            'CELULAR': 'celular',
            'CORREO': 'correo',
            'CARGO ACTUAL': 'cargo_actual',
            'TIPO DE CONTRATACION': 'tipo_contrato',
            'GERENCIA COMERCIAL/ O JEFATURA': 'gerencia_jefatura',
            'NOMBRE JEFE': 'jefe',
            'PROVEEDOR': 'proveedor',
            'CIUDAD': 'ciudad',
            'FECHA INGRESO AREA': 'fecha_ingreso',
            'SEGMENTO': 'segmento',
            'DIRECCION COMERCIAL': 'direccion_comercial',
            'CANAL': 'canal',
            'FECHA RETIRO AREA': 'fecha_retiro_area',
            'MOTIVO': 'motivo',
            'FUENTE': 'fuente'
        })

        df_tabla_hecho_planta['id_estado_registro_hecho'] = 4

        df_tabla_hecho_planta = df_tabla_hecho_planta[['tipo_documento_hecho', 'identificacion_hecho', 'nombre_completo', 'celular', 
                                                       'correo', 'cargo_actual', 'tipo_contrato', 'gerencia_jefatura', 'jefe',
                                                       'proveedor', 'ciudad', 'fecha_ingreso', 'segmento', 'fecha_retiro_area', 'motivo',
                                                       'direccion_comercial', 'canal', 'fuente']]
        
        df_tabla_hecho_planta = limpiezaCamposString(df_tabla_hecho_planta)
        
        df_tabla_hecho_planta['id'] = [generate_uuid().upper() for _ in range(len(df_tabla_hecho_planta))]
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_documento_actual, left_on='tipo_documento_hecho', right_on='tipo_documento', how='left')
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_cargo_actual, left_on='cargo_actual', right_on='cargo_actual', how='left')
        
        columns_to_drop = ['id_estado_registro', 'fecha_creacion', 'fecha_modificacion']
        columns_to_drop = [col for col in columns_to_drop if col in df_tabla_hecho_planta.columns]
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(columns_to_drop, axis=1)

        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_tipo_contratacion_actual, left_on='tipo_contrato', right_on='tipo_contratacion', how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_segmento_actual, left_on='segmento', right_on='segmento', how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_gerencia_jefatura_comercial_actual, left_on='gerencia_jefatura', right_on='gerencia_jefatura', how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_proveedor_actual, left_on='proveedor', right_on='proveedor', how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_fuente, left_on='fuente', right_on='fuente', how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_direccion_comercial_actual, left_on='direccion_comercial', right_on='direccion_comercial', how='left')
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(['id_estado_registro', 'fecha_creacion', 'fecha_modificacion'], axis=1)
        
        df_tabla_hecho_planta['id_direccion_comercial_pr'] = df_tabla_hecho_planta['id_direccion_comercial']
        df_tabla_hecho_planta['jefe'] = df_tabla_hecho_planta['jefe'].apply(lambda x: x.replace('(VACANTE)', '').strip())
        df_tabla_hecho_planta = pd.merge(df_tabla_hecho_planta, df_jefe_actual, left_on='jefe', right_on='nombre', how='left', suffixes=('_left', '_right'))
        
        # Ensure columns exist before dropping
        columns_to_drop = ['id_estado_registro', 'fecha_creacion', 'fecha_modificacion', 'nombre', 'identificacion_left', 'identificacion_right']
        columns_to_drop = [col for col in columns_to_drop if col in df_tabla_hecho_planta.columns]
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop(columns_to_drop, axis=1)
        
        df_tabla_hecho_planta = df_tabla_hecho_planta.drop_duplicates().reset_index(drop=True)
        df_tabla_hecho_planta = df_tabla_hecho_planta.rename(columns={'id_tipo_documento_hecho': 'id_tipo_documento',
                                                                      'identificacion_hecho': 'identificacion',
                                                                      'id_estado_registro_hecho': 'id_estado_registro'})
        
        hoy = datetime.today()
    
        df_tabla_hecho_planta['fecha_creacion'] = hoy
        df_tabla_hecho_planta['fecha_modificacion'] = hoy
        df_tabla_hecho_planta['id_tipo_documento'] = df_tabla_hecho_planta['id_tipo_documento_right']
        df_tabla_hecho_planta['id_estado_registro'] = df_tabla_hecho_planta['id_estado_registro_y']
        df_tabla_hecho_planta['id_direccion_comercial'] = df_tabla_hecho_planta['id_direccion_comercial_pr']
        
        df_tabla_hecho_planta['id_tipo_documento'] = df_tabla_hecho_planta['id_tipo_documento'].fillna(1).astype(int)
        df_tabla_hecho_planta = df_tabla_hecho_planta.loc[:, ~df_tabla_hecho_planta.columns.duplicated()]
        df_tabla_hecho_planta['id_estado_registro'] = 4
        
        df_tabla_hecho_planta = df_tabla_hecho_planta[[
            'id', 'id_tipo_documento', 'identificacion', 'nombre_completo', 'celular', 
            'correo', 'fecha_ingreso', 'fecha_creacion', 'fecha_modificacion', 
            'id_estado_registro', 'id_cargo', 'id_tipo_contrato', 'id_jefe', 'fecha_retiro_area', 'motivo',
            'id_direccion_comercial', 'id_segmento', 'id_gerencia_jefatura', 'ciudad',
            'id_proveedor', 'id_fuente'
        ]]

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta + " | " + par.nombre_hoja_red_inteligencia_comercial_retiro)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(PrepararTablaPrincipalHechointeligencia_comercial_Retiro.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()

    return df_tabla_hecho_planta

# %%
def consultarVersionMaxima():
    """
    Consulta la versión máxima actual de los registros en la base de datos.

    Retorna:
        int: El valor máximo de la columna 'version' en la tabla especificada.
    """
    conn = conexion_BD()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(version) FROM fuentes_cruda.tb_datos_crudos_planta_comercial")
            max_version = cursor.fetchone()[0]  # Extrae el valor máximo
            return max_version if max_version is not None else 0
    except psycopg2.Error as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarVersionMaxima.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        conn.close()

# %%
def agregarVersion(df):
    max_version = consultarVersionMaxima()
    if max_version is None:
        max_version = 0 
    nueva_version = max_version + 1
    df['version'] = nueva_version
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
    log_file = os.path.join(log_directory, "cargue_datos_crudos_planta_comercial.log")
    
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
def importar(archivo_original, archivo_nuevo):
    """
    Copia todas las hojas de un archivo Excel en un nuevo archivo.
    """
    try:
        hojas = pd.read_excel(archivo_original, sheet_name=None, engine="openpyxl")
        with pd.ExcelWriter(archivo_nuevo, engine='openpyxl') as writer:
            for nombre_hoja, df in hojas.items():
                df.to_excel(writer, sheet_name=nombre_hoja, index=False)
        logging.info(f"Archivo copiado en: {archivo_nuevo}")
    except Exception as e:
        logging.error(f"Error al copiar el archivo: {e}")
        raise

# %%
def importarPlantaComercial(ruta, nombre_archivo, hoja_calculo):
    """
    Importa datos de una hoja específica de un archivo Excel.
    """
    try:
        # Construir la ruta completa del archivo
        ruta_completa = os.path.join(ruta, nombre_archivo)

        # Verificar si el archivo existe
        if not os.path.isfile(ruta_completa):
            raise FileNotFoundError(f"El archivo '{ruta_completa}' no fue encontrado.")

        # Leer la hoja específica del archivo Excel
        base_excel = pd.read_excel(ruta_completa, sheet_name=hoja_calculo, engine='openpyxl', dtype=str)

        # Agregar una columna 'FUENTE' con el nombre de la hoja
        base_excel['FUENTE'] = hoja_calculo

        return base_excel
    except Exception as e:
        logging.error(f"Error al importar archivo {ruta_completa}: {e}")
        raise

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
        # Configuración del logging, generación del UUID de ejecución y consulta a la base de datos
        configurarLogging()
        id_ejecucion = generate_uuid().upper()
        print(f'ID de ejecucion del proceso en curso: \033[32m{id_ejecucion}\033[0m')
        
        fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")

        archivo_original = 'C:/Users/INTCOM/OneDrive - Comunicacion Celular S.A.- Comcel S.A/Planta Comercial/PLANTA COMERCIAL NUEVA ESTRUCTURA.xlsx'
        archivo_nuevo = 'C:/ambiente_desarrollo/dev-empresas-negocios-env/fuentes/base_planta_comercial/PLANTA_COMERCIAL.xlsx'

        # Crear una copia del archivo original
        importar(archivo_original, archivo_nuevo)

        # Importar datos principales de planta comercial
        tipos_comerciales = [
            ('red_maestra', par.nombre_hoja_red_maestra),
            ('retiro', par.nombre_hoja_red_retiro),
            ('retail', par.nombre_hoja_red_retail),
            ('directos', par.nombre_hoja_red_directos),
            ('cavs', par.nombre_hoja_red_cavs),
            ('tmk', par.nombre_hoja_red_tmk)
        ]

        df_base_total = pd.DataFrame()

        for tipo, nombre_hoja in tipos_comerciales:
            df_planta_comercial = importarPlantaComercial(par.ruta_fuente_planta_comercial_ajuste, par.nombre_archivo_planta_ajuste, nombre_hoja)
    
            
            if df_planta_comercial is not None:
                registros = len(df_planta_comercial)
                
                
                if registros > 0:
                    # Preparar tabla principal de hecho según el tipo comercial
                    if tipo == 'red_maestra':
                        preparacionCargueTablaJefe(df_planta_comercial,'red_maestra')
                        manejarJefesVacantes(df_planta_comercial)

                        preparacionCargueTablaCoordinadorDirecto(df_planta_comercial,'red_maestra')
                        manejarCoordinadorDirectosVacantes(df_planta_comercial)

                        preparacionCargueTablaGerente(df_planta_comercial,'red_maestra')
                        manejarGerentesVacantes(df_planta_comercial)

                        resultado = preparacionCargueTablasDominioRedMaestra(df_planta_comercial)
                        df_base = PrepararTablaPrincipalHechoRedMaestra(df_planta_comercial)

                        fuentes.append(tipo)
                        registros = len(df_base)
                        cantidad_registros.append(registros)
                    elif tipo == 'retiro':
                        preparacionCargueTablaJefe(df_planta_comercial,'retiro')
                        manejarJefesVacantes(df_planta_comercial)

                        preparacionCargueTablaCoordinadorDirecto(df_planta_comercial,'retiro')
                        manejarCoordinadorDirectosVacantes(df_planta_comercial)

                        preparacionCargueTablaGerente(df_planta_comercial,'retiro')
                        manejarGerentesVacantes(df_planta_comercial)
                        
                        resultado = preparacionCargueTablasDominioRetiro(df_planta_comercial)
                        df_base = PrepararTablaPrincipalHechoRetiro(df_planta_comercial)
                        fuentes.append(tipo)
                        registros = len(df_base)
                        cantidad_registros.append(registros)
                    elif tipo == 'retail':
                        ruta_archivo = par.ruta_fuente_planta_comercial + par.nombre_archivo_planta
                        df = pd.read_excel(ruta_archivo, sheet_name='RETAIL', usecols='A:F')
                        #print(df.head())
                        resultado = preparacionCargueTablasDominioRetail(df)
                        df_base = PrepararTablaPrincipalHechoRetail(df)
                        fuentes.append(tipo)
                        registros = len(df_base)
                        cantidad_registros.append(registros)

                    elif tipo == 'directos':
                        ruta_archivo = par.ruta_fuente_planta_comercial + par.nombre_archivo_planta
                        # Leer solo las columnas A, B y C de la hoja "DIRECTOS"
                        df = pd.read_excel(ruta_archivo, sheet_name='DIRECTOS', usecols='A:C')
                        # Mostrar las primeras filas del DataFrame para verificar
                        #print(df.head())
                        df_base = PrepararTablaPrincipalHechoDirectos(df)
                        fuentes.append(tipo)
                        registros = len(df_base)
                        cantidad_registros.append(registros)
                    elif tipo == 'cavs':
                        resultado = preparacionCargueTablasDominiocavs(df_planta_comercial)
                        df_base = PrepararTablaPrincipalHechoCavs(df_planta_comercial)
                        fuentes.append(tipo)
                        registros = len(df_base)
                        cantidad_registros.append(registros)
                    elif tipo == 'tmk':
                        resultado = preparacionCargueTablasDominioTmk(df_planta_comercial)
                        df_base = PrepararTablaPrincipalHechoTmk(df_planta_comercial)
                        fuentes.append(tipo)
                        registros = len(df_base)
                        cantidad_registros.append(registros)

                    if df_base is not None:
                        df_base_total = pd.concat([df_base_total, df_base], ignore_index=True)
        
        if not df_base_total.empty:
            
            df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, 'planta_comercial', len(df_base_total), 'tb_datos_crudos_planta_comercial', 1)
            df_base_total = agregarVersion(df_base_total)
            cargueDatosBD('tb_datos_crudos_planta_comercial', df_base_total)
        
            fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
            duracion_proceso = fecha_fin_date - fecha_inicio_date
            duracion_proceso_seg = int(duracion_proceso.total_seconds())
            actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date,duracion_proceso_seg)   

        duracion.append(str(duracion_proceso))
        estado.append(1)
        salidaLogMonitoreo()

    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e))
        insertarErroresDB()
    finally: 
        salidaLogMonitoreo()


