# %%
driver_path = "C:\\ambiente_desarrollo\\dev-empresas-negocios-env-web-scraping\\edgedriver_win64\\msedgedriver.exe"
url_login = "https://avanza.claro.com.co/#/signin"
usuario = "38501867"
contrasena = "D4vidp_25*"
download_dir = "C:\\Users\\AMD_INTCOM\\Downloads"
final_dir = r"C:\\ambiente_desarrollo\\dev-empresas-negocios-env-web-scraping\\fuentes"
final_file_name = "Base_Avanza.csv"

# %% [markdown]
# Importar las librerias necesarias

# %%
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import sys
sys.path.append('C:\\ambiente_desarrollo\\dev-empresas-negocios-env-web-scraping\\desarrollo_produccion')
import parametros_produccion as par
import time
import os
import shutil  # Para mover el archivo descargado
import pandas as pd
import uuid
import psycopg2
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

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
destino = 'Web Scraping Avanza'
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

# %% [markdown]
# Funcion de cargue de resumen de los datos

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
        fuentes.append('Web Scraping Avanza')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(cargueResumen.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()
    finally:
        conexion.dispose()

# %% [markdown]
# Funcion que realiza el cargue a BD

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
        nombre_tabla = 'tb_datos_crudos_avanza_webscraping'
        
        df_final.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        fuentes.append('Web Scraping Avanza')
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
        fuentes.append('Web Scraping Avanza')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(insertarErroresDB.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %% [markdown]
# Conexion a BD

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
        
        engine = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_produccion}')
        return engine

    except SQLAlchemyError as e:
        fuentes.append('Web Scraping Avanza')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(conexion_BD.__name__)
        descripcion_error.append(str(e)[:100])
        salidaLogMonitoreo()

# %%
def consultarDatosParametrosDesarrollo():
   
    """
    Metodo que se encarga de consultar cada una de las tablas referente a los pesos de las vacantes
    asignados desde el Micrositio
 
    Argumentos:
        None
    Retorna:
        Tupla de Dataframes consultados
 
    Excepciones manejadas:
        Exception: Registro de control de errores que se presenten para cargue en log de stderr y tabla SQL ErroresProceso
    """
    try:
       
        ConsultarParametrosDesarrollo = "SELECT pd.ParametroId, pd.Nombre, pd.Descripcion, pd.Tipo, pd.Tabla, pd.Valor, \
                                        pd.RangoInicial, pd.RangoFinal, pd.Proceso, pd.EstadoRegistroId, pd.Unidad, pd.Agrupacion, \
                                        FORMAT(pd.FechaModificacion, 'yyyy-MM-dd HH:mm:ss.fff K') AS FechaModificacion \
                                        FROM db_motor_ia.ParametrosDesarrollo pd" #0
        dfParametrosDesarrollo = lecturaDatosAzureSQLMotor(ConsultarParametrosDesarrollo)
 
        return dfParametrosDesarrollo
 
    except Exception as error:
        error_message = str(error)[:300]
        nombre_metodo_error = inspect.currentframe().f_code.co_name
        logging.error(f"Se capturó una excepción: Nombre metodo: {nombre_metodo_error}, Proceso {proceso} Error capturado: {error_message}")
        logging.error("Mensaje de error: ",error_message)
        salidaLogMonitoreo(proceso, nombre_metodo_error, error_message)
 
 
#dfParametrosDesarrollo = consultarDatosParametrosDesarrollo()

# %% [markdown]
# Consulta de historico de datos Web Scraping

# %%
def consultarHistoricoWebScrapingAvanza():
    """
    Función que consulta los datos historicos existentes en la base de datos de la tabla de tb_datos_crudos_legalizadas
    
    Argumentos:
        None
    Retorna: 
        df_avanza_historico : Retorna el historico de los datos cargados en la BD
    Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        
        engine = conexion_BD()
        sql_consulta = "Select * \
                    from fuentes_cruda.tb_datos_crudos_avanza_webscraping"
        df_avanza_historico = pd.read_sql(sql_consulta, engine)
        df_avanza_historico = df_avanza_historico.drop_duplicates(subset=['id','fecha'])
    
        return df_avanza_historico
        
    except Exception as e:
        fuentes.append('Web Scraping Avanza')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(consultarHistoricoWebScrapingAvanza.__name__)
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
    log_directory = par.ruta_log_produccion  # Usa la ruta definida en config.py
    log_file = os.path.join(log_directory, "cargue_webscraping_avanza.log")

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

# %% [markdown]
# Creacion de direccionamiento del driver y objeto para uso de driver, Creacion de espera para ir a Pagina web

# %%
def navegar_Rep_Avanza(driver_path, url_login, usuario, contrasena):
    """
    Función que realiza el inicio de sesión y la navegación hacia el reporte en la plataforma Avanza.

    Argumentos:
        driver_path: Ruta completa al Microsoft Edge WebDriver.
        url_login: URL de la página de inicio de sesión.
        usuario: Nombre de usuario para iniciar sesión.
        contrasena: Contraseña para iniciar sesión.
    
    Retorna:
        driver: Objeto del navegador después de haber navegado al reporte.
    """
    try:
        # Inicializa el WebDriver con el servicio
        service = Service(driver_path)
        driver = webdriver.Edge(service=service)

        # Crea un objeto WebDriverWait
        wait = WebDriverWait(driver, 30)

        # Navega a la página de inicio de sesión
        driver.get(url_login)
        time.sleep(5)

        # Realiza el inicio de sesiónº
        usuario_input = driver.find_element(By.NAME, "username")
        contrasena_input = driver.find_element(By.NAME, "password")
        usuario_input.send_keys(usuario)
        contrasena_input.send_keys(contrasena)

        # Encuentra y haz clic en el botón de inicio de sesión
        boton_logeo = driver.find_element(By.XPATH, "//button[@type='submit']")
        boton_logeo.click()
        time.sleep(3)

        # Selecciona el elemento de la lista desplegable
        lista = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@data-itemvalue='AVANZA']")))
        lista.click()
        time.sleep(3)

        # Haz clic en el enlace "Ver todo"
        ver_todo_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@data-tb-test-id='channel-see-all-link']")))
        ver_todo_link.click()
        time.sleep(5)

        # Haz clic en el enlace del dashboard
        link_dash = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@href='/#/site/AVANZA/redirect_to_view/21434']")))
        link_dash.click()
        time.sleep(5)

        # Navega directamente al reporte detallado
        driver.get("https://avanza.claro.com.co/t/AVANZA/views/DetallereporteCuotas/Comportamiento_Diario?%3Aembed=y&%3AshowVizHome=n&%3Atoolbar=top&%3AopenAuthoringInTopWindow=true&%3AbrowserBackButtonUndo=true&%3AcommentingEnabled=true&%3AreloadOnCustomViewSave=true&%3AshowAppBanner=false&%3AisVizPortal=true&%3AapiID=host0#navType=0&navSrc=Opt&1")
        time.sleep(5)

        return driver  # Retorna el driver para seguir interactuando si es necesario
    
    except Exception as e:
            fuentes.append('Web Scraping Avanza')
            cantidad_registros.append(0)
            estado.append(2)
            funcion_error.append(navegar_Rep_Avanza.__name__)
            descripcion_error.append(str(e)[:100])
            insertarErroresDB()
            salidaLogMonitoreo()
            if 'driver' in locals():
              driver.quit()
            raise

# %%
def descargar_Rep_Avanza(driver, download_dir, final_dir, final_file_name, timeout=300):
    """
      Función que realizara los filtros finales y la descarga de los datos para el uso de DF de Avanza
        
      Argumentos:
        driver: objeto de la anterior funcion para ejecucion.
        download_dir: lugar de descarga del archivo.
        final_dir: lugar final de la descarga del archivo paso a otra carpeta.
        final_file_name: Nombre final que se asignara al archivo.

      Retorna: 
        new_file_path: objeto que se usara para la extraccion del DF del Web Scraping
      Excepciones manejadas: 
        Exception as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:

        wait = WebDriverWait(driver, 30)

        # Reliza los filtros en la visual para la descarga
        filtra_visual = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".tab-vizAxisWrapper"))
        )
        filtra_visual.click()

        boton_descarga = wait.until(
            EC.element_to_be_clickable((By.ID, "download"))
        )
        boton_descarga.click()
        time.sleep(5)

        # Seleccionar la opción "Datos"
        seleccion = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//span[text()='Datos']"))
        )
        seleccion.click()
        time.sleep(3)

        # Cambiar a la nueva ventana
        ventana_original = driver.current_window_handle
        ventana_nueva = None

        for handle in driver.window_handles:
            if handle != ventana_original:
                ventana_nueva = handle
                break

        if ventana_nueva:
            driver.switch_to.window(ventana_nueva)

            # Selección de datos completos y filtros para descarga
            datos_completos = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//div[@aria-label='Datos completos tabla']"))
            )
            datos_completos.click()

            mostrar_campos_btn = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[@class='f10m8oqj icon' and @aria-label='Mostrar campos']"))
            )
            mostrar_campos_btn.click()
            time.sleep(15)

            campo_todo = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//div[@title='(Todo)']"))
            )
            campo_todo.click()
            time.sleep(10)

            descargar_btn = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[span[text()='Descargar']]"))
            )
            descargar_btn.click()
            time.sleep(15)

            # Volver a la ventana original
            driver.switch_to.window(ventana_original)

        # Configurar la descarga del archivo
        time.sleep(180)  # Esperar 3 minutos para la descarga
        initial_files = {f: os.path.getmtime(os.path.join(download_dir, f)) for f in os.listdir(download_dir)}

        elapsed_time = 0
        downloaded_file = None

        while elapsed_time < timeout:
            current_files = os.listdir(download_dir)
            new_files = [f for f in current_files if f.startswith("C_Diario") and "_Datos completos_data" in f and f.endswith(".csv")]

            if new_files:
                downloaded_file = max(new_files, key=lambda x: os.path.getmtime(os.path.join(download_dir, x)))
                break

            time.sleep(2)
            elapsed_time += 2

        if downloaded_file:
            downloaded_file_path = os.path.join(download_dir, downloaded_file)
            new_file_path = os.path.join(final_dir, final_file_name)

            if os.path.exists(new_file_path):
                os.remove(new_file_path)  # Reemplazar archivo existente

            shutil.move(downloaded_file_path, new_file_path)
            print(f"Archivo movido y renombrado a: {new_file_path}")
            return new_file_path  # Retornar el path del archivo final
        else:
            print(f"No se encontró ningún archivo 'C_Diario' en el tiempo establecido.")
            return None

    except Exception as e:
            fuentes.append('Web Scraping Avanza')
            cantidad_registros.append(0)
            estado.append(2)
            funcion_error.append(descargar_Rep_Avanza.__name__)
            descripcion_error.append(str(e)[:100])
            insertarErroresDB()
            salidaLogMonitoreo()


# %% [markdown]
# Modificacion de Data Frame, cambio de tipo de datos

# %%
def CrearDf(id_ejecucion, new_file_path, fecha_inicio): 

    """
    Función que crea el DataFrame principal y realiza la limpieza de los datos para el cargue.

    Argumentos:
        id_ejecucion: ID de ejecución del proceso.
        new_file_path: Ruta del archivo CSV generado por la función de scraping.
        fecha_inicio: Fecha de inicio del procesamiento.

    Retorna: 
        Df_Filtrado: DataFrame con los datos organizados para el cargue.
    """
            
     # Borrar el DataFrame df si existe
     #if 'df' in locals():
     #   print("Eliminando el DataFrame anterior.")
     #   del df  # Eliminar el DataFrame df si existe
    
    # Inicialización de variables
    df = None
    
    try:
        # Cargar el archivo CSV en un DataFrame
        df = pd.read_csv(new_file_path, delimiter=';', on_bad_lines='skip')

        # Agregar columnas adicionales
        df['id_ejecucion'] = id_ejecucion
        df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['fecha_procesamiento'] = fecha_inicio
        df['id_estado'] = 1
        df['id_estado_registro'] = 1
        df.rename(columns={'Mes de FECHA': 'mes_de_fecha'}, inplace=True)
        df.columns = [col.lower() for col in df.columns]  # Convertir nombres de columnas a minúsculas

        # Definir las columnas y tipos deseados
        columnas_ordenadas = [
            'id', 'id_ejecucion', 'tipo', 'mes_de_fecha', 'fechag', 'segmento', 'td', 'vr_neto', 'agent_code', 'canal',
            'cant', 'cfm_actual', 'cfm_anterior', 'ciclo', 'co_id', 'cod_plan_actual', 'cod_plan_anterior', 'custcode_mtr',
            'dia_ev', 'documento2', 'fecha', 'fecha2', 'id_vendedor', 'identif_mtr', 'misisdn', 'plan_actual',
            'plan_anterior', 'rango_cfm', 'razon', 'region', 'region_ant', 'segmento_b', 'ssn', 'subcanal', 'tipob',
            'user_creacion', 'fecha_procesamiento', 'id_estado', 'id_estado_registro'
        ]

        tipos_datos = {
            'id': 'string',
            'id_ejecucion': 'string',
            'tipo': 'string',
            'mes_de_fecha': 'string',
            'fechag': 'Int64',
            'segmento': 'string',
            'td': 'string',
            'vr_neto': 'string',
            'agent_code': 'string',
            'canal': 'string',
            'cant': 'Int64',
            'cfm_actual': 'string',
            'cfm_anterior': 'string',
            'ciclo': 'Int64',
            'co_id': 'Int64',
            'cod_plan_actual': 'string',
            'cod_plan_anterior': 'string',
            'custcode_mtr': 'float',
            'dia_ev': 'Int64',
            'documento2': 'string',
            'fecha': 'string',
            'fecha2': 'Int64',
            'id_vendedor': 'string',
            'identif_mtr': 'Int64',
            'misisdn': 'Int64',
            'plan_actual': 'string',
            'plan_anterior': 'string',
            'rango_cfm': 'string',
            'razon': 'string',
            'region': 'string',
            'region_ant': 'float',
            'segmento_b': 'string',
            'ssn': 'Int64',
            'subcanal': 'string',
            'tipob': 'string',
            'user_creacion': 'string',
            'fecha_procesamiento': 'datetime64[ns]',
            'id_estado': 'Int64',
            'id_estado_registro': 'Int64'
        }

        # Limpiar valores no numéricos o NaN antes de convertir
        for col, dtype in tipos_datos.items():
            if dtype in ['Int64', 'float'] and col in df.columns:
                # Intentar convertir a numérico, reemplazar errores por NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')

                # Reemplazar NaN con un valor predeterminado
                if dtype == 'Int64':
                    df[col] = df[col].fillna(0).astype('int64')  # Convertir a int64 directamente

        # Aplicar tipos a columnas presentes en el DataFrame
        tipos_datos_presentes = {col: dtype for col, dtype in tipos_datos.items() if col in df.columns}
        df = df.astype(tipos_datos_presentes)

        # Reorganizar columnas según el orden deseado
        df = df[[col for col in columnas_ordenadas if col in df.columns]]

        # Limitar el número de filas, si es necesario
        df_filtrado = df

        return df_filtrado

   

    except Exception as e:
        fuentes.append('Web Scraping Avanza')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append(CrearDf.__name__)
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()
        salidaLogMonitoreo()

   

# %% [markdown]
# Ejecucion de funciones Cargue a BD

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
        estado = 1  # O el valor adecuado para el estado
        duracion_proceso_timedelta = fecha_fin_tr - fecha_inicio_tr
        duracion_proceso_seconds = duracion_proceso_timedelta.total_seconds()



        ## Ejecucion primer funcion de Web Scraping logeo y busqueda de datos
        driver = navegar_Rep_Avanza(driver_path, url_login, usuario, contrasena)

        # Ejecucion segunda parte funcion de web scraping, filtro y descarga de los datos
        new_file_path = descargar_Rep_Avanza(driver, download_dir, final_dir, final_file_name)
        
        #Ejecucuion tercera parte del web scraping e inicio ETL, organizacion de la informacion y limpieza
        df_dataframe_avanza = CrearDf(id_ejecucion, new_file_path, fecha_inicio)
        df_avanza_historico = consultarHistoricoWebScrapingAvanza()
         # Convierte la columna 'fecha' a tipo datetime en ambos DataFrames
        df_dataframe_avanza['fecha'] = pd.to_datetime(df_dataframe_avanza['fecha'], errors='coerce')
        df_avanza_historico['fecha'] = pd.to_datetime(df_avanza_historico['fecha'], errors='coerce')
        # Realizar un merge para conservar solo los registros de df_dataframe_avanza que no están en df_avanza_historico
        #df_merge = df_dataframe_avanza.merge(df_avanza_historico[['fecha']], on='fecha', how='left', indicator=True)
        #df_resultante = df_merge[df_merge['_merge'] == 'left_only'].drop(columns=['_merge'])
        # Crear una columna que indique si la fecha existe en el DataFrame histórico
        df_dataframe_avanza['existe_en_historico'] = df_dataframe_avanza['fecha'].isin(df_avanza_historico['fecha']).astype(int)

        # Filtrar solo las filas donde la fecha no está en el histórico
        df_resultante = df_dataframe_avanza[df_dataframe_avanza['existe_en_historico'] == 0].copy()

        # Elimina la columna auxiliar si ya no es necesaria
        df_resultante.drop(columns=['existe_en_historico'], inplace=True)

        registros = len(df_resultante)
        cantidad_registros.append(registros)

        # Ejecucion cargue de datos ETL, se carga la funcion de CargueDatosBD, insercion a BD
        if registros > 0:
           df_resumen = cargueResumen(
        id_ejecucion, fecha_inicio_tr, fecha_fin_tr, duracion_proceso_seconds,
        'Web Scraping Avanza', registros, 'tb_datos_crudos_avanza_webscraping', id_estado
        )
        cargueDatosBD(df_resultante)

    except Exception as e:
        fuentes.append('Web Scraping Avanza')
        cantidad_registros.append(0)
        estado.append(2)
        funcion_error.append("__main__")
        descripcion_error.append(str(e)[:100])
        insertarErroresDB()


