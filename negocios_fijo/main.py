'''
PROYECTO:      EMPRESAS Y NEGOCIOS
AUTOR:         HITSS BI - FERNANDA ZAMBRANO
OPERACIÓN:     ORQUESTADOR DE NEGOCIOS FIJO
VERSIÓN:       V_1.0
FECHA:         02/09/2025
DESCRIPCIÓN:   ORQUESTADOR DE LOS PROCESOS DE NEGOCIOS FIJO
'''

import sys
import os
import pandas as pd
import uuid
from sqlalchemy import text

# Se define la ruta absoluta del proyecto y se agrega a sys.path para encontrar los módulos.
project_root = 'C:/Users/46120442/OneDrive - GLOBAL HITSS/Documentos/Proyectos Empresas y Negocios/HU0019/Desarrollo/'
sys.path.insert(0, project_root)

# Importación de módulos
from utils.bases_genericas import procesador_base  # Clase genérica para procesamiento de tabla auxiliar
from config.config import conIntelienciaComercial, conDbInteligenciaComercial
from sql.negocios_fijos.negocios_fijos import get_query 
from utils.control_ejecucion import ControlEjecucion # Clase genérica para auditoría y logging
from jobs.planta_comercial import red_maestra, retail, directos
import parametros as prm 

def obtener_datos_aux(engine):
    try:
        # Lee y ejecuta la consulta desde la función Python.
        sql_query = get_query()        
        df = pd.read_sql_query(sql_query, engine)# Devuelve la consulta en un DF
        return df
        
    except Exception as e:
        print(f"Error en obtener_datos_aux: {e}")
        raise

def procesos_desde_aux(df_aux, columnas_id):
    """
    Construye el array de procesos dinámicamente desde la tabla auxiliar
    """
    try:
        procesos = [] # Lista para almacenar los procesos de Negocios Fijo      
        # Mapeo de transformadores 
        transformadores = {
            "base_planta_comercial": red_maestra().run_transformacion,
            "base_pc_retail": retail().run_transformacion,
            "base_pc_directos": directos().run_transformacion,
        }        
        # Iterar sobre todos los procesos configurados en parámetros
        for pestana_key, id_base in columnas_id.items():
            print(f"Procesando configuración: {pestana_key}")
            
            # Buscar id_base en tabla auxiliar
            fila = df_aux[df_aux['id_base'] == id_base]
            
            if not fila.empty and pestana_key in transformadores:
                # Extraer información desde la tabla auxiliar
                registro = fila.iloc[0] # Toma el primer registro            
                try:
                    # Extraer el nombre del archivo desde la ruta definida en la tabla aux.
                    ruta_completa = registro.get('nombre_archivo_fuente', '')
                    nombre_archivo = os.path.basename(ruta_completa)              
                    proceso = {
                        "nombre": registro.get('nombre_corto_base', pestana_key.upper()),
                        "pestana_id": pestana_key,
                        "transformador": transformadores[pestana_key],
                        "fuente": nombre_archivo, 
                        "destino": registro.get('nombre_tbl_servidor', f'tabla_{id_base}')  # Tabla destino
                    }                    
                    procesos.append(proceso) # Agrega cada proceso a la lista                
                except KeyError as e:
                    print(f"Error accediendo columna {e} para {pestana_key}")    
             
            elif not fila.empty:
                print(f"id_base {id_base} sin transformación")
            else:
                print(f"No se encontraron datos")        
        print(f"Total de procesos construidos: {len(procesos)}")
        return procesos
        
    except Exception as e:
        print(f"Error en procesos_desde_aux: {e}")
        raise

def ejecutar_proceso(procesador, proceso_config, engine_auditoria, log_folder):
    """Ejecuta un proceso ETL individual y registra log específico"""
    try:
        # Inicialización de variables de control para manejo de errores
        id_proceso = str(uuid.uuid4()).upper() # mantener uuid único por proceso
        control_proceso = None
        proceso_nombre = None
        
        proceso_nombre = proceso_config["nombre"]
        pestana_id = proceso_config["pestana_id"] 
        transformador = proceso_config["transformador"]
        fuente = proceso_config["fuente"]
        destino = proceso_config["destino"]

        # Generar nombre de log específico para cada proceso
        nombre_limpio = pestana_id.replace("base_", "").replace("_", "_")
        log_filename = f"{nombre_limpio}.log"  # Se genera dinamicamente planta_comercial.log, retail.log, etc.

        # Crear control para auditoría específica del proceso con log individual
        control_proceso = ControlEjecucion(
            id_ejecucion=id_proceso,
            db_engine=engine_auditoria,
            log_folder=log_folder,
            log_filename=log_filename,  # Log especifico por proceso
            fuente=fuente, # Para registro en tabla auditoría
            destino=destino, # Para registro en tabla auditoría
        )
        
        # Procesa la pestaña correspondiente y captura cantidad de registros
        cantidad_registros = procesador.procesar_pestana(pestana_id, transformador)
                
        # Registrar éxito de cada proceso en tabla de auditoria 
        if control_proceso:
            control_proceso.registrar_resumen_exitoso(cantidad_registros)
        
        print(f"Proceso {proceso_nombre} completado exitosamente")
        print(f"Registros procesados: {cantidad_registros}")
        return True, 0  # 0 Éxito
        
    except Exception as e:
        print(f"Error en proceso {proceso_config.get('nombre', 'Desconocido')}: {e}")
        
        # Registrar error específico del proceso en tabla de auditoría
        if control_proceso:
            try:
                control_proceso.registrar_error(
                    funcion=f"main.{proceso_nombre}",
                    descripcion=f"Error en {proceso_nombre}: {str(e)}"
                )
            except Exception as error_log:
                print(f"Error adicional: {error_log}")        
        return False, 1 # 1 Fallo

def main():
    """Orquestador principal de todos los procesos de Negocios Fijo"""    
    try:
        # Variables para conexión a base de datos
        engine_datos = None
        engine_auditoria = None

        # Configuración de Rutas definidas en parametros
        log_folder = prm.rutas["logs"] 

        # Conexiones a Base de Datos
        engine_datos = conIntelienciaComercial().pg_ic_connect()
        engine_auditoria = conDbInteligenciaComercial().pg_ic_connect()

        # Configuración desde tabla auxiliar y crear procesador principal
        df_aux= obtener_datos_aux(engine_datos)        
        procesador = procesador_base(engine_datos, df_aux, prm.columnas_id)

        # Definir todos los procesos a ejecutar segun tabla auxiliar
        procesos = procesos_desde_aux(df_aux, prm.columnas_id)
        
        if not procesos:
            print("No se encontraron procesos para ejecutar.")
            return        
        print(f"Iniciando ejecución de {len(procesos)} proceso(s)...")
        
        # Inicializa contadores de procesos a ejecutar
        procesos_exitosos = 0
        total_errores = 0
        
        # Loop principal - ejecuta cada proceso
        for i, proceso_config in enumerate(procesos, 1):
            
            # Ejecuta proceso individual
            exito, errores = ejecutar_proceso(procesador, proceso_config, engine_auditoria, log_folder)
            
            if exito:
                procesos_exitosos += 1
            total_errores += errores
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise
        
    finally:
        # Cerrar conexiones
        if engine_datos:
            try:
                engine_datos.dispose()
            except Exception as e:
                print(f"Error cerrando conexión datos: {e}")
                
        if engine_auditoria:
            try:
                engine_auditoria.dispose()
            except Exception as e:
                print(f"Error cerrando conexión auditoría: {e}")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")