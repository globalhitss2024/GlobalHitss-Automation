'''
PROYECTO:		    EMPRESAS Y NEGOCIOS
AUTOR:			    HITSS BI - JORGE MOSQUERA
OPERACIÓN: 		    MÓDULO CONTINE CLASES PARA PROCESO NEGOCIOS FIJO
VERSIÓN:            V_1.0
FECHA:              15/07/2025
DESCRIPCIÓN:	    CONFIGURACIONES CLASES PARA EXTRACION ARCHIVOS DE EXCEL, CARGAR DATOS Y UPDATE EN TABLAS DE BD.
'''
# Importación de módulos personalizados y configuración de rutas

import pandas as pd
import os
import re
import sys
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine,text
import logging
# Importación de módulos personalizados y configuración de rutas
#sys.path.append('C:/GIT_Empresas_Negocios/GlobalHitss-Automation/Negocios_Fijo/')
import Parametros as prm
# Configuración del logger para registrar mensajes informativos y errores
logger = logging.getLogger(__name__)

# Clase para extraer datos de excel
class ExcelExtractor:
    def __init__(self, df_base):
        self.df_base = df_base
        
    # Metodod que Permite extrae datos de un archivo Excel según el ID base proporcionado.
    def obtener_datos(self, id_base):
        # Seleccionar la fila correspondiente
        fila = self.df_base[self.df_base['id_base'] == id_base].iloc[0]
        archivo = fila['nombre_archivo_fuente']
        hoja_rango = fila['rango']
        # Extraer hoja y rango
        match = re.match(r"(.*?)!(.*)", hoja_rango)
        hoja = match.group(1).strip()
        rango = match.group(2).strip()
        # Extraer solo las columnas del rango (por ejemplo, de 'A1:AR100' a 'A:AR')
        col_match = re.match(r"([A-Z]+)[0-9]*:([A-Z]+)[0-9]*", rango)
        if col_match:
            col_range = f"{col_match.group(1)}:{col_match.group(2)}"
        else:
            raise ValueError(f"Rango no válido: {rango}")
        # Leer Excel con las columnas correctas
        df_excel = pd.read_excel(archivo, sheet_name=hoja, usecols=col_range)
        # Obtener la fecha de modificación del archivo
        fecha_modificacion = datetime.fromtimestamp(os.path.getmtime(archivo))

        return df_excel, fecha_modificacion

# clase de cargar datos y actualizar datos en  postgrest
class load_update_Datos:
    def __init__(self, engine_conexion):
        self.engine_conexion = engine_conexion

    # Metodo que me permite caragr los datos de un df a una tabla en bd posgrest
    def cargar_df_a_tabla(self, df_tabla, name, schema):
        try:
            df_tabla.to_sql(
                name=name,
                con=self.engine_conexion,
                schema=schema,
                if_exists='append',
                index=False
            )
            # print(f"Cargue exitoso a {schema}.{name}")
        except SQLAlchemyError as e:
            print(f"Error al cargar los datos: {e}")

    # metodo para acutalizar campos en tabla auxiliar
    def actualiza_tb_aux_imp_ba(self, fecha_modificacion, fecha_hora_actual, id_base, nombre_tabla_aux):
        try:
            id_base = int(id_base)
            query = text(f"""
                UPDATE {nombre_tabla_aux}
                SET fecha_modificacion_archivo = :fec_mod,
                    fecha_importacion = :fec_imp
                WHERE id_base = :id_base
            """)
            with self.engine_conexion.connect() as conn:
                conn.execute(query, {
                    "fec_mod": fecha_modificacion,
                    "fec_imp": fecha_hora_actual,
                    "id_base": id_base
                })
                conn.commit()
            print("Actualizacion exitosa de fechas.")
        except SQLAlchemyError as e:
            print(f"Error al actualizar los datos: {e}")

# Clase que encapsula el procesamiento de cada pestanna de datos
class ProcesadorBase:
    def __init__(self, engine, df_aux, nombre_tabla_aux):
        self.engine = engine
        self.df_aux = df_aux
        self.nombre_tabla_aux = nombre_tabla_aux
        self.extractor = ExcelExtractor(df_aux)
    
    # Método para procesar una pestanna específica
    def procesar_pestana(self, pestana_id, transformador_func):
        try:
            logger.info(f"Inicio proceso pestanna: {pestana_id}")
            # Extrae los datos desde el archivo Excel
            df, fecha_modificacion = self.extractor.obtener_datos(prm.columnas_id[pestana_id])
            # Obtiene nombre de tabla, esquema e ID base
            name, schema, id_base = asignar_data(self.df_aux, prm.columnas_id[pestana_id])
            # Aplica transformación a los datos
            df_trs = transformador_func(df)
            # Trunca la tabla destino antes de cargar nuevos datos
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {schema}.{name} CASCADE"))
            except Exception as e:
                print(f"Error al limpiar la tabla: {e}")
            # Crea una instancia de la clase con el DataFrame base
            cargador = load_update_Datos(engine_conexion=self.engine)
             # Carga los datos transformados a la tabla destino
            cargador.cargar_df_a_tabla(df_tabla=df_trs, name=name, schema=schema)
            # Captura datos de fecha 
            fecha_hora_actual = datetime.now()
            # Actualiza la tabla auxiliar 
            cargador.actualiza_tb_aux_imp_ba(fecha_modificacion, fecha_hora_actual, id_base, self.nombre_tabla_aux)
            logger.info(f"Fin proceso pestanna: {pestana_id}. Registros cargados: {df_trs.shape[0]}")
        except Exception as e:
            logger.error(f"Error al procesar pestanna {pestana_id}: {e}", exc_info=True)


# Función auxiliar para obtener nombre de tabla, esquema e ID base a procesar
def asignar_data(df, columnas_id):
    fila = df[df['id_base'] == columnas_id]
    if fila.empty:
        raise ValueError("No se encontró ninguna fila con el id_base especificado.")
    return fila.iloc[0]['nombre_tbl_servidor'], fila.iloc[0]['nombre_esquema_servidor'], fila.iloc[0]['id_base']
