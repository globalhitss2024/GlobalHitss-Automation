"""
PROYECTO:             EMPRESAS Y NEGOCIOS
AUTOR:                HITSS BI - JORGE MOSQUERA
OPERACION:            SCRIPT EN PYTHON CON LA ETL PARA LA EXTRACCIONES DE LOS ARCHIVOS REQUERIDOS PARA PROCESO NEGOCIOS FIJO
VERSION:              V_1.0
FECHA:                14/07/2025
DESCRIPCION:          SE CREA SCRIPT CON LAS EXTRACCIONES DE PLANTA COMERCIAL
"""
# Librerías estándar
import Parametros as prm
import sys
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import text
# Importación de módulos personalizados y configuración de rutas
ruta_modulos = prm.ruta_modulos["ruta"]
ruta_modulos_1 = prm.ruta_modulos["rut_cl"]
if ruta_modulos not in sys.path:
    sys.path.append(ruta_modulos)
if ruta_modulos_1 not in sys.path:
    sys.path.append(ruta_modulos_1)    
# Importación de clases y funciones personalizadas
from config import conIntelienciaComercial
from Class_read_trsf_excel import ProcesadorBase

# Permite ver el todos los campos de un df
pd.set_option('display.max_columns', None)

# Configuración del logger para registrar mensajes informativos y errores
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Función reutilizable para transformar columnas de las pestannas
def transformar_columna(df, columna, reemplazar_nbsp=False, valor_nulo='NO REGISTRA'):
    def limpiar(x):
        if pd.isna(x) or str(x).strip() == '':
            return valor_nulo
        texto = str(x).strip()
        if reemplazar_nbsp:
            texto = texto.replace('&amp;amp;nbsp;', '')
        return texto.upper()
    return df[columna].apply(limpiar)

# Funcion de transformacion campos pestanna red maestra
def transformar_Campos_redmaestra(df_pestanna):
    df_transformado = pd.DataFrame()
    df_transformado['id_planta_comercial'] = df_pestanna['Consecutivo']
    for original, (nuevo, reemplazar_nbsp) in prm.columnas_trsf_redmaestrar.items():
        df_transformado[nuevo] = transformar_columna(df_pestanna, original, reemplazar_nbsp)
    df_transformado['fecha_ingreso'] = pd.to_datetime(
        df_pestanna['FECHA DE INGRESO A LA COMPAÑIA'], errors='coerce'
    ).fillna(pd.Timestamp('1900-01-01'))
    df_transformado['codigo_rr'] = df_pestanna['CODIGO RR O HFC'].apply(
        lambda x: 'PENDIENTE' if pd.isna(x) or str(x).strip() == '' else str(x).strip().upper()
    )
    df_transformado = df_transformado[
        df_pestanna['Consecutivo'].notna() &
        df_pestanna['CEDULA/NIT'].apply(lambda x: not (pd.isna(x) or str(x).strip() == ''))
    ]
    #print(df_transformado)
    return df_transformado

# Funcion de transformacion campos pestanna retail
def transformar_campos_retail(df_transformado):
    columnas_nuevas = []
    for original, (nuevo, reemplazar_nbsp) in prm.columnas_trsf_retail.items():
        df_transformado[nuevo] = transformar_columna(df_transformado, original, reemplazar_nbsp)
        columnas_nuevas.append(nuevo)
        #print(f"Transformando columna: {original} -> {nuevo}")
    # Filtrar solo las columnas nuevas
    df_transformado = df_transformado[columnas_nuevas]
    # print(f"Datos del DataFrame transformado:\n{df_transformado}")
    return df_transformado

# Obtiene los datos de la tabla auxiliar de importación
def obtener_datos_aux(engine):
    with engine.begin() as conn:
        result = conn.execute(text(prm.Planta_Comercial["tb_aux_imp"]))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    nombre_tabla_aux = prm.Planta_Comercial["tb_aux_imp"].split("from")[1].split("where")[0].strip()
    return df, nombre_tabla_aux

######################### Función principal MAIN orquesta la ejecución del proceso ETL#######################

def main():
    try:
        # Conexión a la base de datos
        engine = conIntelienciaComercial().pg_ic_connect()
        # Obtiene los datos auxiliares y nombre de la tabla auxiliar
        df_aux, nombre_tabla_aux = obtener_datos_aux(engine)
        # Crea instancia clase procesador
        procesador = ProcesadorBase(engine, df_aux, nombre_tabla_aux)
        # proceso pestanna RED MAESTRA
        procesador.procesar_pestana("base_planta_comercial", transformar_Campos_redmaestra)
        # proceso pestanna RETAIL
        #procesador.procesar_pestana("base_pc_retail", transformar_campos_retail)


    except Exception as e:
        logger.error("Error general del proceso", exc_info=True)


if __name__ == '__main__':
    main()
