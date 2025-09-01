'''
PROYECTO:		    EMPRESAS Y NEGOCIOS
AUTOR:			    HITSS BI - FERNANDA ZAMBRANO
OPERACIÓN: 		    MÓDULO CLASES PARA EXTRAER Y CARGAR ARCHIVOS EXCEL
VERSIÓN:            V_1.0
FECHA:              15/07/2025
DESCRIPCIÓN:	    CONFIGURACIONES CLASES PARA EXTRACION Y CARGAR DATOS DE ARCHIVOS DE EXCEL EN TABLAS DE BD.
'''
# Importación de módulos personalizados y configuración de rutas

import pandas as pd
import re
from sqlalchemy.exc import SQLAlchemyError
import logging

# Configuración del logger para registrar mensajes informativos y errores
logger = logging.getLogger(__name__)

#Clase que extrae datos de excel 
class excel_extractor:
    def __init__(self, archivo, hoja_rango):
        self.archivo = archivo
        self.hoja_rango = hoja_rango

    def obtener_datos(self):
        # Extraer hoja y rango
        match = re.match(r"(.*?)!(.*)", self.hoja_rango)
        hoja = match.group(1).strip()
        rango = match.group(2).strip()
        # Extraer solo las columnas del rango (por ejemplo, de 'A1:AR100' a 'A:AR')
        col_match = re.match(r"([A-Z]+)[0-9]*:([A-Z]+)[0-9]*", rango)
        if col_match:
            col_range = f"{col_match.group(1)}:{col_match.group(2)}"
        else:
            raise ValueError(f"Rango no válido: {rango}")
        # Leer Excel con las columnas correctas
        df_excel = pd.read_excel(self.archivo, sheet_name=hoja, usecols=col_range)

        return df_excel

# clase de cargar datos y actualizar datos en BD
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
            raise
