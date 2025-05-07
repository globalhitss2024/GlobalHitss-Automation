# %%
import pandas as pd
import openpyxl as opn
import psycopg2
from psycopg2 import sql, Error, OperationalError
import uuid
from datetime import datetime
import os
import shutil  # Para mover el archivo descargado
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import sys
import ast
sys.path.append('C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\desarrollo_produccion')
import parametros_produccion as par
pd.set_option('display.max_columns', None)
# Ruta al archivo Excel
ruta_excel = r'C:\\ambiente_desarrollo\\dev-empresas-negocios-env\\fuentes\\base_marca_blanca\\BASE ASIGNACION EMPRESAS.xlsx'

# %%
#VARIABLES GLOBALES
fecha_actual = datetime.today().date()
duracion = []
fuentes = []
cantidad_registros = []
estado = []
fecha_fin_procesamiento =[]
funcion_error = []
descripcion_error = []
id_ejecucion = str(uuid.uuid4())  # Generar UUID de ejecución
destino = 'Marca Blanca'
id_estado = '1'

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
# Función para cargar resumen de datos en la BD
def cargueResumen(id_ejecucion, fecha_inicio_date,fecha_fin_procesamiento, duracion,fuentes, cantidad_registros, destino, id_estado):
    try:
        df_resumen_cargue = pd.DataFrame({
        'id_ejecucion': [id_ejecucion],  # Envolver en una lista
        'fecha_inicio_procesamiento': [fecha_inicio_date],
        'fecha_fin_procesamiento': [fecha_fin_procesamiento], 
        'duracion_segundos': [duracion],
        'fuentes': [fuentes],
        'cantidad_registros': [cantidad_registros],
        'destino': [destino],
        'id_estado': [id_estado],
    })
        Usuario_pro = 'postgres'
        contraseña_pro = '1Nt3l163nC14_C0m3rc14L'
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_produccion}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos 
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_resumen_cargue'
        
        df_resumen_cargue.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
    
    except SQLAlchemyError as e:
        fuentes.append('Marca Blanca')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueResumen.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

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
        
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_produccion}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'fuentes_cruda'
        nombre_tabla = 'tb_datos_crudos_marca_blanca'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append('Marca Blanca')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueDatosBD.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
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
        
        conexion_errores = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_produccion}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_errores_cargue'
        errores.to_sql(nombre_tabla, con=conexion_errores, schema=nombre_esquema, if_exists='append', index=False)
        cargueResumen(id_ejecucion_en_curso, fecha_inicio_tr,2) 
        salidaLogMonitoreo()

    
    except SQLAlchemyError as e:
        fuentes.append('Marca Blanca')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def conexion_BD():
    try:
        connection = psycopg2.connect(
            dbname="dwh_db",    
            user="45110947",            
            password="Mmilu28()*",    
            host="100.123.59.140",          
            port="5432"                
        )
        print("Conexión a la base de datos Yellowbrick exitosa.")
        return connection
    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None




# %%
def consultarHistoricoMb():
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_legalizadas
    
    Argumentos:
        None
    Retorna: 
        df_historico_mb : Retorna el historico de los datos cargados en la BD
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        engine = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_produccion}')
        #engine = conexion_BD()
        sql_consulta = "Select * \
                    from fuentes_cruda.tb_datos_crudos_marca_blanca"
        df_historico_mb = pd.read_sql(sql_consulta, engine)
    
    
        return df_historico_mb
        
    except Exception as e:
        fuentes.append('Marca Blanca')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarHistoricoMb.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    finally:
        engine.dispose()

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
    log_directory = par.ruta_log_produccion  # Usa la ruta definida en config.py
    log_file = os.path.join(log_directory, "cargue_marcablanca.log")

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
def consulta_mb(engine):
    try:
        # Consulta tabla TBL_CLIEN_SOLUCIONES_CORP
        consulta = f"""
        SELECT *
        FROM dwh_db.CLIENTES.TBL_CLIEN_SOLUCIONES_CORP
        """
        
        cursor = engine.cursor()
        cursor.execute(consulta)
        resultados = cursor.fetchall()

        # Obtener los nombres de las columnas
        columnas = [desc[0] for desc in cursor.description]

        # Crea un DataFrame con los resultados y los nombres de columnas
        df_resultados = pd.DataFrame(resultados, columns=columnas)

        return df_resultados

    except Exception as e:
        fuentes.append('Marca Blanca')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consulta_mb.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
# Función para limpiar los datos
def limpiar_data_mb(df):
    try:
        # Verificar si las columnas 'valor_paq' y 'valor_cfm' existen
        if 'valor_paq' not in df.columns or 'valor_cfm' not in df.columns:
            raise ValueError("Una o ambas columnas ('valor_paq', 'valor_cfm') no existen en el DataFrame.")
        
        # Asegurarse de que las columnas 'valor_paq' y 'valor_cfm' estén en formato numérico
        df['valor_paq'] = pd.to_numeric(df['valor_paq'], errors='coerce')
        df['valor_cfm'] = pd.to_numeric(df['valor_cfm'], errors='coerce')
        
        # Elimina los decimales (mantiene solo la parte entera) y convierte a int
        df['valor_paq'] = df['valor_paq'].fillna(0).astype(int) 
        df['valor_cfm'] = df['valor_cfm'].fillna(0).astype(int)  
        
        return df

    except Exception as e:
        fuentes.append('Marca Blanca')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(limpiar_data_mb.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

# obtener los datos y manejo de la limpieza
def ejecutar_proceso(engine):
    try:
        # Obteniene los resultados de la consulta
        resultado = consulta_mb(engine)
        
        if resultado is None:
            print("No se obtuvieron resultados de la consulta.")
            return
        
        # Aplicar la función de limpieza
        resultado_limpio = limpiar_data_mb(resultado)
        
        if resultado_limpio is None:
            print("No se pudo realizar la limpieza de datos.")
            return
        
        print("Limpieza realizada.")
        
    except Exception as e:
        fuentes.append('Marca Blanca')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(ejecutar_proceso.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()



# %%
def cargar_asignacion_empresas(ruta_excel):
    try:
        # Leer el archivo Excel de asignación de empresas
        df_asignacion_bajas = pd.read_excel(ruta_excel)
        
        # Verifica si la columna 'NIT' existe en el Excel
        if 'NIT' not in df_asignacion_bajas.columns:
            print("La columna 'NIT' no se encuentra en el archivo Excel.")
            return None
        
        return df_asignacion_bajas
    
    except Exception as e:
        fuentes.append('Marca blanca')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargar_asignacion_empresas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()
    


# %%
def consultarDatosParametrosDesarrollo():
    try:

  
        engine = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_produccion}')

        #engine = conexion_BD()
        sql_consulta = "Select * \
                    from control_procesamiento.reglas_negocio"
        df_reglas = pd.read_sql(sql_consulta, engine)
    
    
        return df_reglas
    
    except Exception as e:
        fuentes.append('Marca blanca')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarDatosParametrosDesarrollo.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
# Extraer los datos de bajas con los filtros especificos
def extraer_datos_bajas(dfa, df_ParametrosDesarrollo):
    try:

        #Creacion Variable de parametros para los filtros constantes
        df_filtro_par = df_ParametrosDesarrollo[df_ParametrosDesarrollo['nombre'] == 'valor_paq_mb']
        val_paq = int(df_filtro_par['valor'].values[0])
        df_filtro_par2 = df_ParametrosDesarrollo[df_ParametrosDesarrollo['nombre'] == 'excluidos_mb']
        excluidos_texto = df_filtro_par2['valor'].values[0]  
        excluidos = ast.literal_eval(excluidos_texto)
        
        # Filtra por 'fecha_desact_paq' que NO esté vacía (excluir NaN)
        df_filtrado_ba = df_filtrado = dfa[~dfa['fecha_desact_paq'].isna()]
        
        #df_filtro_bajas = df_filtrado_ba[df_filtrado_ba['fecha_desact_paq'].str.contains('2024-11', na=False)]
        df_filtro_bajas = df_filtrado_ba[df_filtrado_ba['fecha_desact_paq'].dt.strftime('%Y-%m') == '2024-12']

        # Filtra por 'anio_mes_act_paq' que no sea igual a '2024-11'
        df_filtro_bajas = df_filtro_bajas[df_filtro_bajas['anio_mes_act_paq'] != '2024-12']
        # Filtra por 'valor_paq' mayor que 0
        df_filtro_bajas = df_filtro_bajas[df_filtro_bajas['valor_paq'] > val_paq]
        

        #excluidos = ['Claro Directo', 'CLARO DIRECTO', 'Servicio Datos', 'Servicio Telefonia GSM']
        df_filtro_bajas = df_filtro_bajas[~df_filtro_bajas['desc_serv_instalado'].isin(excluidos)]
        
        # Ordenar el DataFrame por las columnas 'co_id', 'tel_num', 'cod_paq_instalado' de forma ascendente
        df_filtro_bajas = df_filtro_bajas.sort_values(by=['co_id', 'tel_num', 'cod_paq_instalado'], ascending=True)
        # Establecer la fecha base de Excel (1 de enero de 1900)
        fecha_base_excel = pd.to_datetime('1900-01-01')
        # Crear una nueva columna con el número de serie de Excel para la fecha
        df_filtro_bajas['fecha_act_paq_excel'] = (df_filtro_bajas['fecha_act_paq'] - fecha_base_excel).dt.days + 2  # Sumamos 2 por el ajuste de Excel
        df_filtro_bajas['fecha_desac_paq_excel'] = (df_filtro_bajas['fecha_desact_paq'] - fecha_base_excel).dt.days + 2  # Sumamos 2 por el ajuste de Excel
        # Crear la columna de concatenación
        df_filtro_bajas['concat_duplicados'] = df_filtro_bajas['co_id'].astype(str) + \
            df_filtro_bajas['tel_num'].astype(str) + \
            df_filtro_bajas['cod_paq_instalado'].apply(lambda x: str(int(x)) if pd.notna(x) else '') + \
            df_filtro_bajas['fecha_desac_paq_excel'].astype(str) + \
            df_filtro_bajas['valor_paq'].astype(str)
        # Crear la columna de concatenación por servicio
        df_filtro_bajas['concat_duplicados_por_servicio'] = df_filtro_bajas['co_id'].astype(str) + \
            df_filtro_bajas['fecha_desac_paq_excel'].astype(str) + \
            df_filtro_bajas['valor_paq'].astype(str) + \
            df_filtro_bajas['desc_serv_instalado'].astype(str)
        
        df_filtro_bajas['concat_com_bajas_altas'] = df_filtro_bajas['nit'].astype(str) + \
            df_filtro_bajas['tel_num'].astype(str) + \
            df_filtro_bajas['cod_paq_instalado'].apply(lambda x: str(int(x)) if pd.notna(x) else '') + \
            df_filtro_bajas['valor_paq'].astype(str)
        # Verifica si el DataFrame filtrado tiene datos
        # Identificar duplicados en 'concat_duplicados_por_servicio'
        df_filtro_bajas['duplicado_por_servicio'] = df_filtro_bajas['concat_duplicados_por_servicio'].duplicated(keep=False)  # `keep=False` marca todos los duplicados
        df_filtro_bajas['numero_duplicado'] = df_filtro_bajas.groupby('concat_duplicados_por_servicio').cumcount() + 1
        # Filtrar solo aquellos donde el número de duplicado es 1 (es decir, el primero)
        df_bajas = df_filtro_bajas[df_filtro_bajas['numero_duplicado'] == 1]
        df_bajas['Segmento'] = 'NEGOCIOS'
        df_bajas['cruce_bajas_altas'] = 'No cruza'
        df_bajas['Tipo'] = 'BAJAS'

        df_asignacion_bajas = cargar_asignacion_empresas(ruta_excel)
        if df_asignacion_bajas is not None:
            # Comparar el NIT entre df_altas y el archivo Excel, y asignar 'EMPRESAS' si coincide
            df_bajas.loc[df_bajas['nit'].isin(df_asignacion_bajas['NIT']), 'Segmento'] = 'EMPRESAS'
        df_bajas.loc[df_bajas['nit'].isin(df_asignacion_bajas['NIT']), 'Segmento'] = 'EMPRESAS'
        
         # Filtra por 'fecha_desact_paq' que esté vacía
        df_filtrado_al_a = dfa[dfa['fecha_desact_paq'].isna()]
        # Filtra por 'anio_mes_act_paq' distinto de '2024-11'
        df_filtrado_altas_a = df_filtrado_al_a[df_filtrado_al_a['anio_mes_act_paq'] == '2024-12']
        # Filtra por 'valor_paq' mayor que 0
        df_filtrado_altas_a = df_filtrado_altas_a[df_filtrado_altas_a['valor_paq'] > 0]
        # Ordenar el DataFrame por las columnas 'co_id', 'tel_num', 'cod_paq_instalado' de forma ascendente
        df_filtrado_altas_a = df_filtrado_altas_a.sort_values(by=['co_id', 'tel_num', 'cod_paq_instalado'], ascending=True)
        
        df_filtrado_altas_a['concat_com_bajas_altas'] = df_filtrado_altas_a['nit'].astype(str) + \
              df_filtrado_altas_a['tel_num'].astype(str) + \
              df_filtrado_altas_a['cod_paq_instalado'].apply(lambda x: str(int(x)) if pd.notna(x) else '') + \
              df_filtrado_altas_a['valor_paq'].astype(str)
        # Crear una columna 'Segmento' con valor por defecto 'NEGOCIOS'
        df_bajas.loc[df_bajas['concat_com_bajas_altas'].isin(df_filtrado_altas_a['concat_com_bajas_altas']),'cruce_bajas_altas'] = 'Cruza'

   
        return df_bajas

    except Exception as e:
        fuentes.append('Marca Blanca')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(extraer_datos_bajas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
def extraer_Datos_Altas(df,df_ParametrosDesarrollo):
    try:


            #Creacion Variable de parametros para los filtros constantes
        df_filtro_par = df_ParametrosDesarrollo[df_ParametrosDesarrollo['nombre'] == 'valor_paq_mb']
        val_paq = int(df_filtro_par['valor'].values[0])
        df_filtro_par2 = df_ParametrosDesarrollo[df_ParametrosDesarrollo['nombre'] == 'excluidos_mb']
        excluidos_texto = df_filtro_par2['valor'].values[0]  # Obtiene el texto de la "lista"
        excluidos = ast.literal_eval(excluidos_texto)

        # Filtra por 'fecha_desact_paq' que esté vacía
        df_filtrado_al = df[df['fecha_desact_paq'].isna()]
        # Filtra por 'anio_mes_act_paq' distinto de '2024-11'
        df_filtrado_altas = df_filtrado_al[df_filtrado_al['anio_mes_act_paq'] == '2024-12']
        # Filtra por 'valor_paq' mayor que 0
        df_filtrado_altas = df_filtrado_altas[df_filtrado_altas['valor_paq'] > val_paq]
        # Elimina filas donde 'desc_serv_instalado' tenga ciertos valores específicos
        #excluidos = ['Claro Directo', 'CLARO DIRECTO', 'Servicio Datos', 'Servicio Telefonia GSM']
        df_filtrado_altas = df_filtrado_altas[~df_filtrado_altas['desc_serv_instalado'].isin(excluidos)]
        # Ordenar el DataFrame por las columnas 'co_id', 'tel_num', 'cod_paq_instalado' de forma ascendente
        df_filtrado_altas = df_filtrado_altas.sort_values(by=['co_id', 'tel_num', 'cod_paq_instalado'], ascending=True)
        # Establecer la fecha base de Excel (1 de enero de 1900)
        fecha_base_excel = pd.to_datetime('1900-01-01')
        # Crear una nueva columna con el número de serie de Excel para la fecha
        df_filtrado_altas['fecha_act_paq_excel'] = (df_filtrado_altas['fecha_act_paq'] - fecha_base_excel).dt.days + 2  # Sumamos 2 por el ajuste de Excel
        # Crear la columna de concatenación
        df_filtrado_altas['concat_duplicados'] = df_filtrado_altas['co_id'].astype(str) + \
            df_filtrado_altas['tel_num'].astype(str) + \
            df_filtrado_altas['cod_paq_instalado'].apply(lambda x: str(int(x)) if pd.notna(x) else '') + \
            df_filtrado_altas['fecha_act_paq_excel'].astype(str) + \
            df_filtrado_altas['valor_paq'].astype(str)
        # Crear la columna de concatenación por servicio
        df_filtrado_altas['concat_duplicados_por_servicio'] = df_filtrado_altas['co_id'].astype(str) + \
            df_filtrado_altas['fecha_act_paq_excel'].astype(str) + \
            df_filtrado_altas['valor_paq'].astype(str) + \
            df_filtrado_altas['desc_serv_instalado'].astype(str)
        # Crea columna para identificar cruce contra bajas
        df_filtrado_altas['concat_com_bajas_altas'] = df_filtrado_altas['nit'].astype(str) + \
        df_filtrado_altas['tel_num'].astype(str) + \
        df_filtrado_altas['cod_paq_instalado'].apply(lambda x: str(int(x)) if pd.notna(x) else '') + \
        df_filtrado_altas['valor_paq'].astype(str)
        # Identificar duplicados en 'concat_duplicados_por_servicio'
        df_filtrado_altas['duplicado_por_servicio'] = df_filtrado_altas['concat_duplicados_por_servicio'].duplicated(keep=False)  # `keep=False` marca todos los duplicados
        # Asignar un número de secuencia a cada grupo de duplicados basado en la columna 'concat_duplicados_por_servicio'
        df_filtrado_altas['numero_duplicado'] = df_filtrado_altas.groupby('concat_duplicados_por_servicio').cumcount() + 1
        # Filtrar solo aquellos donde el número de duplicado es 1 (es decir, el primero)
        df_altas = df_filtrado_altas[df_filtrado_altas['numero_duplicado'] == 1]
        # Crear una columna 'Segmento' con valor por defecto 'NEGOCIOS'
        df_altas['Segmento'] = 'NEGOCIOS'
        df_altas['cruce_bajas_altas'] = 'No cruza'
        df_altas['Tipo'] = 'ALTAS'
        #Leer el archivo Excel de asignación de empresas
           #Leer el archivo Excel de asignación de empresas
        df_asignacion_bajas = cargar_asignacion_empresas(ruta_excel)
        if df_asignacion_bajas is not None:
            # Comparar el NIT entre df_altas y el archivo Excel, y asignar 'EMPRESAS' si coincide
            df_altas.loc[df_altas['nit'].isin(df_asignacion_bajas['NIT']), 'Segmento'] = 'EMPRESAS'
        df_altas.loc[df_altas['nit'].isin(df_asignacion_bajas['NIT']), 'Segmento'] = 'EMPRESAS'


            # Filtrado para las bajas
        df_filtrado_ba_a = df[~df['fecha_desact_paq'].isna()]  # Filtramos solo las filas con fecha_desact_paq no nula
        # Aseguramos que 'fecha_desact_paq' sea string antes de aplicar .str.contains()
        df_filtrado_ba_a['fecha_desact_paq'] = df_filtrado_ba_a['fecha_desact_paq'].fillna('').astype(str)
        # Filtra por 'anio_mes_act_paq' distinto de '2024-11' (no es necesario modificar esta parte)
        df_filtrado_bajas_a = df_filtrado_ba_a[df_filtrado_ba_a['fecha_desact_paq'].str.contains('2024-12', na=False)]
        # Filtra por 'anio_mes_act_paq' que no sea igual a '2024-11'
        df_filtrado_bajas_a = df_filtrado_bajas_a[df_filtrado_bajas_a['anio_mes_act_paq'] != '2024-12']
        # Filtra por 'valor_paq' mayor que 0
        df_filtrado_bajas_a = df_filtrado_bajas_a[df_filtrado_bajas_a['valor_paq'] > 0]
        # Identificar duplicados en 'concat_duplicados_por_servicio'
        df_filtrado_bajas_a['concat_com_bajas_altas'] = df_filtrado_bajas_a['nit'].astype(str) + \
        df_filtrado_bajas_a['tel_num'].astype(str) + \
        df_filtrado_bajas_a['cod_paq_instalado'].apply(lambda x: str(int(x)) if pd.notna(x) else '') + \
        df_filtrado_bajas_a['valor_paq'].astype(str)
        # Ahora, el cruce con el DataFrame de Altas
        df_altas.loc[df_altas['concat_com_bajas_altas'].isin(df_filtrado_bajas_a['concat_com_bajas_altas']),'cruce_bajas_altas'] = 'Cruza'
        
                

        
        return df_altas 

    except Exception as e:
        fuentes.append('Marca Blanca')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(extraer_Datos_Altas.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()


# %%
if __name__ == "__main__":
    try:
        configurarLogging()
        #Variables constantes dentro del codigo para funciones
        

        id_ejecucion = str(uuid.uuid4()).upper()  # Generar ID de ejecución
        fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_inicio_tr = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")
        fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_fin_tr = datetime.strptime(fecha_fin, "%Y-%m-%d %H:%M:%S")
        id_estado = 1
        estado = []  # O el valor adecuado para el estado
        duracion_proceso_timedelta = fecha_fin_tr - fecha_inicio_tr
        duracion_proceso_seconds = duracion_proceso_timedelta.total_seconds()
        
        df_ParametrosDesarrollo = consultarDatosParametrosDesarrollo()   
        engine = conexion_BD()
        resultado = consulta_mb(engine)
        ejecutar_proceso(engine)
        resultado_limpio = limpiar_data_mb(resultado) 

        mb_bajas = extraer_datos_bajas(resultado_limpio,df_ParametrosDesarrollo)
        mb_Altas = extraer_Datos_Altas(resultado_limpio,df_ParametrosDesarrollo)
        mb_historico = consultarHistoricoMb()


        # Concatenar los DataFrames
        df_mb = pd.concat([mb_bajas, mb_Altas], axis=0, ignore_index=True)

        # Filtrar por 'cruce_bajas_altas' == 'No cruza'
        df_mb = df_mb[df_mb['cruce_bajas_altas'] == 'No cruza']
        df_mb['id_ejecucion'] = id_ejecucion  # Agregar la columna id_ejecucion con el mismo UUID en todas las fila
        df_mb['id'] = [str(uuid.uuid4()) for _ in range(len(df_mb))]  # Agregar la columna id con un UUID único por cada fila
        df_mb['fecha_procesamiento'] = fecha_inicio  # Agregar la columna fecha_procesamiento con la fecha y hora actual
        df_mb['id_estado'] = 1  # Crear el estado del registro como entero
        df_mb['id_estado_registro'] = 1
        df_mb.columns = [col.lower() for col in df_mb.columns]  # Convertir los nombres de las columnas a minúsculas
        df_mb.drop(columns=['duplicado_por_servicio', 'cruce_bajas_altas', 'numero_duplicado'], inplace=True)
        df_mb['mes_mb'] = df_mb.apply(
        lambda row: row['anio_mes_act_paq'] if row['tipo'] == 'ALTAS' 
        else (str(row['fecha_desact_paq'])[:7] if pd.notnull(row['fecha_desact_paq']) else None) 
        if row['tipo'] == 'BAJAS' 
        else None, axis=1)

        df_marca_blanca_a = pd.merge(df_mb, mb_historico[['concat_duplicados_por_servicio', 'mes_mb']], 
                    on=['concat_duplicados_por_servicio', 'mes_mb'], 
                    how='left', 
                    indicator=True)
        
        df_marca_blanca = df_marca_blanca_a[df_marca_blanca_a['_merge'] == 'left_only'].drop(columns=['_merge'])
        
        registros = len(df_marca_blanca)
        cantidad_registros.append(registros)

        # Ejecucion cargue de datos ETL, se carga la funcion de CargueDatosBD, insercion a BD
        if registros > 0:
           df_resumen = cargueResumen(
        id_ejecucion, fecha_inicio_tr, fecha_fin_tr, duracion_proceso_seconds,
        'Marca Blanca', registros, 'tb_datos_crudos_marca_blanca', id_estado
        )
        cargueDatosBD(df_marca_blanca)
  
    except Exception as e:
        fuentes.append('Marca Blanca')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()

# %%
#df_marca_blanca.head(5)


