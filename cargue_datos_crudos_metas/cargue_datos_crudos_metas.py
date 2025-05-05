# %%
"""
***************************************************************************************
* CLARO  HITSS - EMPRESAS Y NEGOCIOS                                                  *
* OBJETIVO: Extración de fuentes crudas de Metas                                      * 
*           y cargue a base de datos de forma automatica                              *
*           Comunicacion Celular S.A.- Comcel S.A\Wilmer Camargo Ochoa - Data_PCC     *
* TABLA DE INGESTA POSTGRESQL: tb_datos_crudos_metas                                  *
* FECHA CREACION: 15 de Julio de 2024                                                 *
* ELABORADO POR: JEFFERSON ROZO                                                       *
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
import sys 
from openpyxl import load_workbook
pd.set_option('display.max_columns', None) 

# %%
#VARIABLES GLOBALES
fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
fecha_actual = datetime.today().date()
duracion = []
fuentes = []
cantidad_registros = []
destino = [par.destino_metas]
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
        fuentes.append(par.nombre_archivo_metas)
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
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,par.nombre_archivo_metas,0,par.destino_metas,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append(par.ruta_fuente_metas)
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
        fuentes.append(par.nombre_archivo_metas)
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
        fuentes.append(par.nombre_archivo_metas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(generate_uuid.__name__)
        insertarErroresDB()
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()


# %%
def consultarTablaHistorico(tabla_consulta):
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
        fecha_actual = datetime.now().strftime("%Y-%m-%d")
        sql_consulta = f"""
            SELECT fecha_procesamiento
            FROM fuentes_cruda.{tabla_consulta} 
            WHERE DATE(fecha_procesamiento) = '{fecha_actual}'
        """

        df_tabla_bd = pd.read_sql(sql_consulta, engine)

        #print(f"Resultado de la consulta: {df_tabla_bd}")  

        return df_tabla_bd
     
       
    except Exception as e:
        fuentes.append(par.nombre_archivo_planta)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarTablaHistorico.__name__)
        descripcion_error.append(str(e))
        insertarErroresDB()
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
        fuentes.append(par.nombre_archivo_metas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(archivo_modificado_hoy.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def importarMetas(ruta, hoja_calculo_canal_fijo, hoja_calculo_canal_movil, hoja_calculo_red, hoja_calculo_bajas_fijo):
    """
    Este metodo realiza la importacion de las metas de la fuente cruda
    Argumentos:
        ruta: ruta donde se encuentra el archivo
        nombre_archivo: nombre del archivo
        hoja_calculo_canal_fijo: nombre de la hoja de calculo del canal fijo
        hoja_calculo_canal_movil: nombre de la hoja de calculo del canal movil
        hoja_calculo_red: nombre de la hoja de calculo de la red
        hoja_calculo_bajas_fijo: nombre de la hoja de calculo de las bajas fijo
    Retorna: 
        df: dataframe con los datos de la fuente cruda
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        files = os.listdir(ruta)
        #print(f'rutas de archivos: {files}')
        #print('\n')
        # Unir la ruta con el nombre del archivo para obtener la ruta completa
        full_paths = [os.path.join(ruta, file) for file in files]

        # Obtener el archivo más reciente de la ruta
        #newest_file = max(full_paths, key=os.path.getctime) #comentariado el dia 18/03/2024 por Mario Puello, para evitar busqueda recursiva en carpetas dentro de la ruta especificada
        newest_file = [f for f in os.listdir(ruta) if archivo_modificado_hoy(os.path.join(ruta, f))and f.startswith('Metas_Negocios') and f.endswith('.xlsx')] #adicion 18/03/2024, Mario Puello para el uso de solo la ruta especifica de ubicacion del archivo fuente
        print(f'rutas de archivos: {ruta+newest_file[0]}')#adicion 18/03/2024, Mario Puello para pruebas de visualizacion de ruta del archivo fuente

 
        # Leer las hojas de cálculo especificadas
        base_excel_canal_fijo = pd.read_excel(ruta+newest_file[0], sheet_name=hoja_calculo_canal_fijo, engine='openpyxl')
        base_excel_canal_movil = pd.read_excel(ruta+newest_file[0], sheet_name=hoja_calculo_canal_movil, engine='openpyxl')
        base_excel_red = pd.read_excel(ruta+newest_file[0], sheet_name=hoja_calculo_red, engine='openpyxl')
        base_excel_bajas_fijo = pd.read_excel(ruta+newest_file[0], sheet_name=hoja_calculo_bajas_fijo, engine='openpyxl')
        print('lectura de acrchivo')
        return base_excel_canal_fijo, base_excel_canal_movil, base_excel_red, base_excel_bajas_fijo
    
    except Exception as e:
        # Asegúrate de definir estas listas o variables en algún lugar antes de utilizarlas.
        fuentes.append(newest_file)  # Si no se define 'par', uso 'newest_file'
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(importarMetas.__name__)
        descripcion_error.append(str(e)[:100])
        
        # Las funciones 'insertarErroresDB' y 'salidaLogMonitoreo' deben estar implementadas
        insertarErroresDB()  # Esta función debe estar definida
        salidaLogMonitoreo()  # Esta función debe estar definida también
   

# %%
def combinarMetas(df_canal_fijo, df_canal_movil, df_red, df_bajas_fijo):
    """
    Este metodo realiza la union de los dataframes de las metas
    Argumentos:
        df_canal: dataframe del canal
        df_red: dataframe de la red
        df_tipo: dataframe del tipo
    Retorna: 
        df: dataframe con los datos de la fuente cruda
    Excepciones manejadas:
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try :
        # Se crea una columna para identificar el tipo de fuente
        df_canal_fijo['Hoja_Fuente'] = 'META-'+ par.nombre_hoja_canal_fijo
        df_canal_movil['Hoja_Fuente'] = 'META-'+ par.nombre_hoja_canal_movil
        df_red['Hoja_Fuente'] = 'META-'+ par.nombre_hoja_red
        df_bajas_fijo['Hoja_Fuente'] = 'META-'+ par. nombre_hoja_bajas_fijo

        #Se Crea una columna detalle para todas los Dataframe donde contiene el nombre de la columna del detalle
        df_canal_fijo['Detalle'] = 'Canal'
        df_canal_movil['Detalle'] = 'Canal'
        df_red['Detalle'] = 'Red'
        df_bajas_fijo['Detalle'] = 'Crecimiento'

        # Se combinan los dataframes de Canal, Red y Tipo
        df_combinado = pd.concat([df_canal_fijo, df_canal_movil, df_red, df_bajas_fijo], ignore_index=True)

        #Se Crea la columna Tipo, donde se contienen los datos de las hojas
        #df_combinado['Tipo'] = df_combinado['Canal'].fillna('') + df_combinado['Red'].fillna('') + df_combinado['Crecimiento'].fillna('')
        #df_combinado.drop(columns=['Canal', 'Red', 'Crecimiento'], inplace=True)
        df_combinado = df_combinado.rename(columns={'Red': 'Tipo'})
        df_combinado['Tipo'] = df_combinado['Tipo'].fillna('')

        #Cambiar mes de nombre a número
        print('Cambiar mes a numero')
        #print(df_combinado.head(10))
       

        df_combinado['Mes'] = df_combinado['Mes'].map({'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4, 'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8, 'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12})
        print('mes')
        df_combinado['CC'] = df_combinado['CC'].fillna('11').astype(int)#SE AGREGA LINEA DE CODIGO 09/04/2025
        print(df_combinado['CC'].dtypes)
        df_combinado['CC'] = df_combinado['CC'].astype(str).str.replace(' ', '')#SE AGREGA LINEA DE CODIGO 09/04/2025
        df_combinado.loc[df_combinado['CC'].str.contains('vacante', case=False, na=False),'CC']='11' #adicion 18/03/2024, Mario Puello para eliminacion de cedulas string=vacantes
        print(0) 
        
        df_combinado.loc[df_combinado['Nombre'].str.contains('vacante', case=False, na=False),'CC']='11'
        print(1) # adicion 09/04/2025 Mario Puello para eliminacion de cedulas string=vacantes y Johana Perez
         # se modifica 0 por 11
        df_combinado.to_excel(r"C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\fuentes\\base_metas\\pruebas_datos_crudos 2.xlsx")
        print(2)
        
        df_combinado['CC'] = df_combinado['CC'].astype(int)
        print(3)
        df_combinado.to_excel(r"C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\fuentes\\base_metas\\pruebas_datos_crudos 2.xlsx")


        # Se reordenan las columnas
        print('reordenar columnas')
        df_combinado = df_combinado[['CC', 'Nombre', 'Mes', 'Hoja_Fuente', 'Detalle', 'Tipo', 'Meta']]
         
        return df_combinado
    
    except Exception as e:
        fuentes.append(par.nombre_archivo_metas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(combinarMetas.__name__)
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
        print('funcion de cargue')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'fuentes_cruda'
        nombre_tabla = 'tb_datos_crudos_metas'
        df_final.to_excel(r"C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\fuentes\\base_metas\\pruebas_datos_crudos.xlsx")
        df_final.to_sql(nombre_tabla, con = conexion, schema=nombre_esquema, if_exists='append', index=False)

    
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_metas)
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
        fuentes.append(par.nombre_archivo_metas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueResumen.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %%
def seleccionCamposMetas(df_combinado,fecha_inicio_date,id_ejecucion):
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
        df_base['indice_columna'] = df_base.index #adicion 18/03/2025 por Mario Puello, para la generacion de una columna de indice que ayude a generar uuid unicos con datos iguales de en la fuente

        df_base['anio'] = pd.to_datetime(fecha_inicio_date).year
        df_base['id'] = [generate_uuid().upper() for _ in range(len(df_base))]
        df_base['id_ejecucion'] = id_ejecucion
        df_base['fecha_procesamiento'] = fecha_inicio_date
        df_base['fuente'] = par.nombre_archivo_metas
        #df_base['id_estado'] = 1
        df_base['id_estado_registro'] = 1

        df_base = df_base.rename(columns={
            'CC': 'identificacion',
            'Nombre': 'nombre',
            'Mes': 'mes',
            'Hoja_Fuente': 'hoja_fuente',
            'Tipo': 'tipo',
            'Meta': 'couta_mes',
            'Detalle': 'detalle'
        })
        
        df_base = df_base[['id', 'id_ejecucion', 'identificacion', 'nombre', 'anio', 'mes', 'hoja_fuente', 'tipo', 'couta_mes', 'fecha_procesamiento', 'fuente', 'id_estado_registro', 'detalle']]
        df_base['identificacion'] = df_base['identificacion'].astype('int64')
        
        return df_base
    
    except Exception as e:
        fuentes.append(par.nombre_archivo_metas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(seleccionCamposMetas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

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

        query = "UPDATE fuentes_cruda.tb_datos_crudos_metas SET id_estado_registro = 4 WHERE id_estado_registro = 1"
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
def crucePlantaComercial(df_base):
    """
    Función que se encarga de cruzar la identificacion del comercial con su id_cargo

    Argumentos:
        df_base: Contiene el dataframe que se requiere cruzar estos campos
    Retorna: 
        df_final: Retorna el dataframe con los campos faltantes
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    conn = conexion_BD()

    if conn:
        
        try:
            with conn.cursor() as cur:
                #cur.execute("SELECT identificacion, id_cargo FROM fuentes_cruda.tb_datos_crudos_planta_comercial WHERE version = ( SELECT MAX(version) FROM fuentes_cruda.tb_datos_crudos_planta_comercial ) and identificacion != 0 and id_cargo != 0") #Comentariado 18/03/2025 por mario puello, operadores de comparacion errados en consulta sql postgresql
                cur.execute("SELECT identificacion, id_cargo FROM fuentes_cruda.tb_datos_crudos_planta_comercial WHERE version = ( SELECT MAX(version) FROM fuentes_cruda.tb_datos_crudos_planta_comercial ) and identificacion <> 0 and id_cargo <> 0") #correccion de consulta 18/03/2025 por mario puello, ver comentario anterior
                rows = cur.fetchall()

                df_resultados = pd.DataFrame(rows, columns=['identificacion', 'id_cargo'])
                df_resultados=df_resultados.drop_duplicates(subset=['identificacion', 'id_cargo']) #adicion 19/03/2025 por Mario Puello, para eliminar registros duplicados de identificacion con el mismo cargo en planta comercial
                #df_resultados.to_excel(r"C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\fuentes\\base_metas\\pruebas_datos_RESULTADOS.xlsx")
                #df_base.to_excel(r"C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\fuentes\\base_metas\\pruebas_datos_crudos_PRE_cruce_planta.xlsx")
                # Perform the cross-reference and add id_cargo to the base dataframe
                df_final = pd.merge(df_base, df_resultados, on='identificacion', how='left')
                df_final['id_cargo'] = df_final['id_cargo'].fillna(0)
                df_final['id_cargo'] = df_final['id_cargo'].astype(int)
                #df_final.to_excel(r"C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\fuentes\\base_metas\\pruebas_datos_crudos_POS_cruce_planta.xlsx")
                df_final = df_final[['id', 'id_ejecucion', 'identificacion', 'nombre','id_cargo', 'anio', 'mes', 'hoja_fuente', 'tipo', 'couta_mes', 'fecha_procesamiento', 'fuente', 'id_estado_registro', 'detalle']]
                
                INT_MIN = -2_147_483_648
                INT_MAX = 2_147_483_647
                df_final.loc[df_final['identificacion'] > INT_MAX, 'identificacion'] = INT_MIN
                

                return df_final
            
        except SQLAlchemyError as e:
            fuentes.append(par.nombre_archivo_metas)
            cantidad_registros.append(0)
            estado.append(2)
            funcion_error.append(crucePlantaComercial.__name__)
            descripcion_error.append(str(e)[:100])
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
    log_file = os.path.join(log_directory, "cargue_datos_crudos_metas.log")

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
        archivos = [f for f in os.listdir(par.ruta_fuente_metas) if archivo_modificado_hoy(os.path.join(par.ruta_fuente_metas, f))]
        archivos_actualizados = archivos
        
        # Consultar histórico
        df_historico = consultarTablaHistorico('tb_datos_crudos_metas')

        # Asegurar que la columna 'fecha_procesamiento' sea de tipo datetime
        print('inicio de proceso')
        df_historico['fecha_procesamiento'] = pd.to_datetime(df_historico['fecha_procesamiento'], errors='coerce')

        # Verificar si la fecha de hoy ya está en el histórico
        if fecha_inicio_date.date() in df_historico['fecha_procesamiento'].dt.date.unique():
            print("Ya hay registros en el histórico. No se almacenará nada.")
        else:
            print(archivos_actualizados)
            if par.nombre_archivo_metas in archivos_actualizados:
                
                # Importar metas
                print('\n')
                print(f'ruta fuente metas: {par.ruta_fuente_metas}')
                print(f'nombre del archivo: {par.nombre_hoja_canal_fijo}')
                print('\n')
                df_canal_fijo, df_canal_movil, df_red, df_bajas_fijo = importarMetas(par.ruta_fuente_metas, par.nombre_hoja_canal_fijo, par.nombre_hoja_canal_movil, par.nombre_hoja_red, par.nombre_hoja_bajas_fijo)

                # Combinar metas
                df_combinado = combinarMetas(df_canal_fijo, df_canal_movil, df_red, df_bajas_fijo)
                print('Combinar metas')
                # Limpieza de campos
                df_limpiado = limpiezaCamposString(df_combinado)
                print('Limpieza de campos')
                # Seleccionar campos
                df_base = seleccionCamposMetas(df_limpiado, fecha_actual, id_ejecucion_en_curso)
                print('Seleccionar campos')
                df_metas_nuevo = crucePlantaComercial(df_base)
                
                registros = len(df_metas_nuevo)
                cantidad_registros.append(registros)

                if registros > 0:
                    print(f'cantidad de registros{registros}')
                    df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_metas, registros, par.destino_metas, 1)
                    cambioDeEstado()
                    
                    cargueDatosBD(df_metas_nuevo)

                fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
                duracion_proceso = fecha_fin_date - fecha_inicio_date
                duracion_proceso_seg = int(duracion_proceso.total_seconds())
                actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date, duracion_proceso_seg)
                
            else:#INCLUSION DE CODIGO ELSE MARIO PUELLO 16/01/2024
                duracion_proceso =None

            duracion.append(str(duracion_proceso))
            estado.append(1)
            salidaLogMonitoreo()
    except Exception as e:
        fuentes.append(par.nombre_archivo_metas)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()


