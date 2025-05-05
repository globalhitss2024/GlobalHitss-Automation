# %%
"""
***************************************************************************************
* CLARO  HITSS - EMPRESAS Y NEGOCIOS                                                  *
* OBJETIVO: Data Cruda base cloud                                                     *                                                                            *
* TABLA DE INGESTA POSTGRESQL: tbl_crudo_cloud                                        *
* FECHA CREACION: 22 de mayo de 2024                                                  *
* ELABORADO POR: DANILO RODRIGUEZ                                                     *
* *************************************************************************************
* MODIFICACIONES
* NOMBRE                   FECHA      VERSION            DESCRIPCION
* Johana Perez             05/05/2024   V1.1            Cambiar lectura en la hoja a "base ventas Cloud y se Aplican los filtros 
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
destino = [par.destino_cloud]
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
        fuentes.append(par.nombre_archivo_cloud)
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
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,par.nombre_archivo_cloud,0,par.destino_cloud,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_cloud)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def importarCloudExcel(ruta, nombre_archivo, hoja_calculo):
    """
    Función que se encarga de importar archivos en formato de excel
    
    Argumentos:
        ruta: variable que contiene la ruta de la fuente
        nombre_archivo: Nombre del archivo
    Retorna: 
        base_excel: Dataframe con los datos provenientes del excel
    Excepciones manejadas: 
        SQLAlchemyError as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    
    try:
        base_excel = pd.read_excel(ruta+nombre_archivo, sheet_name=hoja_calculo)
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_cloud)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(importarCloudExcel.__name__)
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
        fuentes.append(par.nombre_archivo_cloud)
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
        fuentes.append(par.nombre_archivo_cloud)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(actualizarFechaFinProcesamiento.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def consultarCloudHistorico():
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_cloud
    
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
                    from fuentes_cruda.tb_datos_crudos_cloud"
        df_cloud_historico = pd.read_sql(sql_consulta, engine)
        df_cloud_historico = df_cloud_historico.drop_duplicates(subset=['llave',])
    
        return df_cloud_historico
        
    except Exception as e:
        fuentes.append(par.nombre_archivo_cloud)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarCloudHistorico.__name__)
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
        fuentes.append(par.nombre_archivo_cloud)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(generate_uuid.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# %%
def limpiezaNumerica(valor):
    """
    Esta función limpia y convierte valores a enteros, asegurándose de que cualquier valor no numérico se transforme en 0.

    Argumentos:
        El valor que se intentará convertir a un entero.

    Retorna:
        El valor convertido a entero si es numérico, de lo contrario, retorna 0.

    Excepciones manejadas:
        Si ocurre cualquier excepción durante la conversión, se captura la excepción,se registra el error y se actualizan listas de monitoreo para seguimiento de errores.
    """

    try:
        return int(valor) if str(valor).isdigit() else 0
    
    except Exception as e:
        #fuentes.append(par.nombre_archivo_cloud)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(generate_uuid.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
def limpiezaCamposCloud(base, fecha_inicio_date, id_ejecucion):
    """
    Función que se encarga de limpiar los datos del dataframe cloud. Y crea campos adicionales necesarios para el control de los datos
    
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
        df_base.rename(columns = {
            'LLAVE': 'llave', 
            'FECHA DE VENTA': 'fecha_de_venta', 
            'MES': 'mes',
            'AÑO': 'ano',
            'RED': 'red',
            'ID CLIENTE': 'id_cliente',
            'RAZON SOCIAL': 'razon_social',
            'NIT': 'nit',
            'TIPO DE VENTA': 'tipo_de_venta',
            'SEGMENTO': 'segmento',
            'SEGMENTO GENERAL': 'segmento_general',
            'ASIGNACIÓN CRM': 'asignacion_crm',
            'CUSTOMER ID': 'customer_id',
            'IDSUSCRIPTION': 'idsuscription',
            'ORDEN': 'ot',
            'SKU\nPARALLELS': 'sku_parallels',
            'TIPOSERVICIO': 'tipo_servicio',
            'PRODUCTO': 'producto',
            'DESCRIPCIÓN PLAN': 'descripcion_plan',
            'No SERVICIOS': 'no_servicios',
            'VALOR UNITARIO SIN IVA': 'valor_unitario_sin_iva',
            'VALOR UNITARIO IVA INCLUIDO': 'valor_unitario_iva_incluido',
            'VALOR TOTAL SIN IVA': 'valor_total_sin_iva',
            'VALOR TOTAL IVA INCLUIDO': 'valor_total_iva_incluido',
            'CEDULA COMERCIAL': 'cedula_comercial',
            'NOMBRES': 'nombres',
            'GERENCIA': 'gerencia',
            'JEFATURA': 'jefatura',
            'CORDINACIÓN': 'cordinacion',
            'CARGO': 'cargo',
            'CONTRATACIÓN': 'contratacion',
            'PROVEEDOR': 'proveedor',
            'CIUDAD': 'ciudad',
            'ESTADO COMERCIAL INICIAL DEL SERVICIO VENDIDO': 'estado_comercial_inicial_del_servicio_vendido',
            'ESTADO COMERCIAL FINAL DEL SERVICIO VENDIDO': 'estado_comercial_final_del_servicio_vendido',
            'OBSERVACIONES COMERCIAL / NOMBRE DOMINIO': 'observaciones_comercial_nombre_dominio',
            'PROMOCIONES': 'promociones',
            'FECHA ACTIVACION PARALLELS DEL SERVICIO': 'fecha_activacion_parallels_del_servicio',
            'MES ACTIVACION PARALLELS DEL SERVICIO': 'mes_activacion_parallels_del_servicio',
            'AÑO ACTIVACION PARALLELS': 'ano_activacion_parallels',
            'NOMBRE INGENIERO ACTIVACION PA': 'nombre_ingeniero_activacion_pa',
            'FECHA LEGALIZACION VENTAS': 'fecha_legalizacion_ventas',
            'MES LEGALIZACION VENTAS': 'mes_legalizacion_ventas',
            'AÑO LEGALIZACION': 'ano_legalizacion',
            'COD. INCIDENTE ONYX': 'cod_incidente_onyx',
            'COD. OT GENERADA': 'cod_ot_generada',
            'OBSERVACIONES': 'observaciones',
            'VALOR TOTAL SIN IVA MOVIMIENTOS RECURRENTES UP GRADE': 'valor_total_sin_iva_movimientos_recurrentes_up_grade',
            'VALOR UNITARIO SIN IVA2': 'valor_unitario_sin_iva2',
            'VALOR UNITARIO IVA INCLUIDO3': 'valor_unitario_iva_incluido3',
            'VALOR TOTAL SIN IVA2': 'valor_total_sin_iva2',
            'VALOR TOTAL IVA INCLUIDO5': 'valor_total_iva_incluido5',
            'TIPO DE ACTIVACIÓN': 'tipo_de_activacion',
            'VALOR EN EL SISTEMA (ODIN)': 'valor_en_el_sistema_odin',
            'TIPO DE RENTA\n (ONE TIME - ANUAL - MENSUAL)': 'tipo_de_renta_one_time_anual_mensual',
            'VALOR TOTAL SIN IVA NORMALIZADO\n(VALOR ODIN/12) Todos los Productos': 'valor_total_sin_iva_normalizado_valor_odin_12_todos_los_produ',
            'SEGMENTO DE ALTA': 'segmento_de_alta','DIRECTOR': 'director',
            'Nombre de Base': 'nombre_de_base',
            'Reto Estrategico SI/NO': 'reto_estrategico'
        }, inplace=True)
        limpieza_columnas = [
            'id_cliente', 'customer_id', 'idsuscription', 'no_servicios', 
            'valor_unitario_sin_iva', 'valor_unitario_iva_incluido', 
            'valor_total_sin_iva', 'valor_total_iva_incluido', 
            'cedula_comercial', 'valor_unitario_sin_iva2', 
            'valor_unitario_iva_incluido3', 'valor_total_sin_iva2', 
            'valor_total_iva_incluido5', 'valor_en_el_sistema_odin', 
            'valor_total_sin_iva_normalizado_valor_odin_12_todos_los_produ'
        ]
        campos_tabla = [
            'llave', 'mes','tipo_red','razon_social','nit','tipo_de_venta','segmento','segmento_general','asignacion_crm','idsuscription','sku_parallels','tipo_servicio','producto','descripcion_plan','nombres','gerencia','jefatura','cordinacion','cargo','contratacion',
            'proveedor','ciudad','estado_comercial_inicial_del_servicio_vendido','estado_comercial_final_del_servicio_vendido','observaciones_comercial_nombre_dominio',
            'promociones','mes_activacion_parallels_del_servicio','nombre_ingeniero_activacion_pa','observaciones','tipo_de_activacion','tipo_de_renta_one_time_anual_mensual',
            'segmento_de_alta','director','reto_estrategico'
        ]
        for campo in campos_tabla:
            if campo in df_base.columns:
                df_base[campo] = df_base[campo].astype(str).str.upper().str.strip().str.replace('\n', '', regex=True).str.replace('\r', '', regex=True).str.replace('\t', '', regex=True).str.replace('  ', '', regex=True)
        df_base['cedula_comercial'] = df_base['cedula_comercial'].replace(0, 11) 
        df_base['reto_estrategico'] = df_base['reto_estrategico'].replace({'NO': 'NO', 'SI': 'SI'}, regex=True)
        df_base.loc[~df_base['reto_estrategico'].isin(['SI', 'NO']), 'reto_estrategico'] = 'NO'
        df_base[limpieza_columnas] = df_base[limpieza_columnas].applymap(limpiezaNumerica)
        df_base.insert(5, 'tipo_red', ['CLOUD'] * len(df_base))
        df_base['id'] = df_base.apply(lambda row: generate_uuid().upper(), axis=1)
        df_base['id_ejecucion'] = id_ejecucion
        df_base['fecha_procesamiento'] = fecha_inicio_date
        df_base['fuente'] = 'CLOUD CORREOS'
        df_base['id_estado'] = '1'
        orden_columnas = ['id', 'id_ejecucion'] + [col for col in df_base.columns if col not in ['id', 'id_ejecucion']]
        df_base = df_base.reindex(columns=orden_columnas)

        
    except Exception as e:
        fuentes.append(par.nombre_archivo_cloud)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(limpiezaCamposCloud.__name__)
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
        nombre_tabla = 'tb_datos_crudos_cloud'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append(par.nombre_archivo_cloud)
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
        fuentes.append(par.nombre_archivo_cloud)
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
    log_file = os.path.join(log_directory, "cargue_datos_crudos_cloud.log")

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
        archivos = [f for f in os.listdir(par.ruta_fuente_cloud) if archivo_modificado_hoy(os.path.join(par.ruta_fuente_cloud, f))]
        archivos_actualizados = archivos
        df_cloud_historico = consultarCloudHistorico()

        if par.nombre_archivo_cloud in archivos_actualizados:  
                id_ejecucion = generate_uuid().upper()
                id_ejecucion_en_curso = id_ejecucion
                
                # Cambiar la hoja a "base ventas Cloud"
                base_excel_cloud = importarCloudExcel(par.ruta_fuente_cloud, par.nombre_archivo_cloud, "base ventas Cloud") # codigo agregado por Johana Perez
                
                # Aplicar los filtros indicados
                base_excel_cloud = base_excel_cloud[   # codigo agregado por Johana Perez
                    (base_excel_cloud['SEGMENTO DE ALTA'].str.contains("negocios", case=False, na=False)) &  # codigo agregado por Johana Perez
                    (base_excel_cloud['ESTADO COMERCIAL INICIAL DEL SERVICIO VENDIDO'].str.contains("Activo Parallels", case=False, na=False)) &  # codigo agregado por Johana Perez
                    (base_excel_cloud['ESTADO COMERCIAL FINAL DEL SERVICIO VENDIDO'].str.contains("Activo", case=False, na=False))   # codigo agregado por Johana Perez
                ]  # codigo agregado por Johana Perez

                #base_excel_cloud = importarCloudExcel(par.ruta_fuente_cloud, par.nombre_archivo_cloud, par.nombre_hoja_cloud)
                df_limpieza_campos_cloud = limpiezaCamposCloud(base_excel_cloud,fecha_inicio_date,id_ejecucion)
                df_historico = consultarCloudHistorico()
                columnas_a_eliminar = [
                'FECHA CONFIGURACION INICIAL DEL SERVICIO',
                'MES CONFIGURACION INICIAL',
                'AÑO CONFIGURACION INICIAL',
                'NOMBRE INGENIERO CONFIGURACION SERVICIO',
                'FECHA ACTIVO SERVICIOS PROFESIONALES',
                'MES ACTIVO SERVICIOS PROFESIONALES',
                'AÑO ACTIVO SERVICIOS PROFESIONALES',
                'FECHA ACTIVACION FINAL DEL SERVICIO',
                'MES ACTIVACION FINAL DEL SERVICIO',
                'AÑO ACTIVACION FINAL',
                'No ORDEN ID CANCELACION SERVICIO',
                'FECHA DE CANCELACION DEL SERVICIO',
                'MES CANCELACION DEL SERVICIO',
                'AÑO CANCELACION DEL SERVICIO',
                'COD. DE ENLACE ONYX',
                'MOVIMIENTOS RECURRENTES DOWN GRADE',
                'MOVIMIENTOS RECURRENTES UP GRADE',
                'VALOR TOTAL SIN IVA MOVIMIENTOS RECURRENTES DOWN GRADE',
                'NO. PEDIDO MOVIMIENTOS RECURRENTES DOWN GRADE',
                'NO. PEDIDO MOVIMIENTOS RECURRENTES UPG RADE',
                'SERVICIOS CLARO FIJO',
                'SERVICIO',
                'ING. IT',
                'FECHA VENCIMIENTO CODIGO PROMOCIONAL',
                'Permanencia',
                'SEGMENTO DE BAJA ',
                'OPERACION']
                df_limpieza_campos_cloud = df_limpieza_campos_cloud.drop(columns=columnas_a_eliminar)
                df_merged = pd.merge(df_limpieza_campos_cloud, df_historico[['llave']], on='llave', how='outer', indicator=True)
                df_cloud_nuevo = df_merged[df_merged['_merge'] == 'left_only'].copy()
                df_cloud_nuevo.drop(columns=['_merge'], inplace=True)
                
                #df_cloud_nuevo = pd.merge(df_limpieza_campos_cloud, df_cloud_historico, left_on='llave', right_on= ['llave'], how='outer', indicator=True, suffixes=('_cloud', '_historico')).query('_merge == "left_only"')
                #columnas_seleccionadas = [par.mapeo_columnas_cloud[col] for col in par.mapeo_columnas_cloud.keys() if par.mapeo_columnas_cloud[col] in df_cloud_nuevo.columns]
                #df_cloud_nuevo = df_cloud_nuevo[columnas_seleccionadas]
                #df_cloud_nuevo.columns = df_cloud_nuevo.columns.str.replace('_cloud', '')
                fuentes.append(par.nombre_archivo_cloud)
                registros = len(df_cloud_nuevo)
                cantidad_registros.append(registros)
                
                if registros > 0:
                    df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_cloud,registros,par.destino_cloud,1)
                    cargueDatosBD(df_cloud_nuevo)
                else:
                    df_resumen = cargueResumen(id_ejecucion, fecha_inicio_date, par.nombre_archivo_cloud,registros,par.destino_cloud,1)
                    cargueDatosBD(df_cloud_nuevo)

                fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                fecha_fin_date = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
                duracion_proceso = fecha_fin_date - fecha_inicio_date
                duracion_proceso_seg = int(duracion_proceso.total_seconds())
                actualizarFechaFinProcesamiento(id_ejecucion, fecha_fin_date,duracion_proceso_seg)

        duracion.append(str(duracion_proceso))
        estado.append(1)
        salidaLogMonitoreo()

    except Exception as e:
        fuentes.append(par.nombre_archivo_cloud)
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()


