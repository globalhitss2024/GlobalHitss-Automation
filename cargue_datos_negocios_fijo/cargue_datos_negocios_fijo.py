# %%
# Importación de librerías necesarias
import pandas as pd
from pandas import concat as concat_func
import psycopg2
import datetime as dt
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import sys
sys.path.append('C:/ambiente_desarrollo/dev-empresas-negocios-env/desarrollo_produccion')
import logging
import os
import uuid
import numpy as np
import parametros_produccion as par
#from reglas_negocio import *
pd.set_option('display.max_columns', None)

# %%
# Conexión a la base de datos "inteligencia_comercial"
conn_inteligencia_comercial = psycopg2.connect(
    host="100.123.42.210",
    port="5432",
    database="inteligencia_comercial",
    user="postgres",
    password="1Nt3l163nC14_C0m3rc14L"
)

# Conexión a la base de datos "DBInteligenciaComercial"
conn_db_inteligencia_comercial = psycopg2.connect(
    host="100.123.42.210",
    port="5432",
    
    database="DBInteligenciaComercial",
    user="postgres",
    password="1Nt3l163nC14_C0m3rc14L"
)

# Conexión a la base de datos "DBInteligenciaComercialDesarrollo"
conn_db_inteligencia_comercial_desarrollo = psycopg2.connect(
    host="100.123.42.210",
    port="5432",
    
    database="DBInteligenciaComercialDesarrollo",
    user="postgres",
    password="1Nt3l163nC14_C0m3rc14L"
)

# %%
# Consulta para cargar la tabla tb_calendario
query_tb_calendario_temp = """
SELECT *
FROM fuentes_cruda.tb_calendario_temp;
"""

df_tb_calendario_temp = pd.read_sql(query_tb_calendario_temp, conn_db_inteligencia_comercial_desarrollo)

# %%
# Aseguramos que 'Fecha' esté en formato datetime y Dia_Habil como int
df_tb_calendario_temp['Fecha'] = pd.to_datetime(df_tb_calendario_temp['Fecha'])
df_tb_calendario_temp['Dia_Habil'] = df_tb_calendario_temp['Dia_Habil'].astype(int)

# Hoy
hoy = datetime.now().date()

# Definir el rango de fechas
if hoy.weekday() == 0:  # lunes
    fecha_inicio = hoy - timedelta(days=3)  # viernes
else:
    fecha_inicio = hoy - timedelta(days=1)  # ayer

fecha_fin = hoy - timedelta(days=1)  # siempre ayer como límite superior

# Asignar rangos como string para la consulta SQL
fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')

# Mostrar rango
print(f"📅 Consulta entre fechas: {fecha_inicio_str} y {fecha_fin_str}")

# %%
# Configurar las fechas (solo fechas, sin horas)
#fecha_fin = datetime.now().date() - timedelta(days=1)  # Ayer
#fecha_inicio = fecha_fin - timedelta(days=0)

#fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d')
#fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')

# Mostrar el rango que se está usando
print(f"Consulta entre fechas: {fecha_inicio_str} y {fecha_fin_str}")

# Consulta ajustada para comparar solo la parte de la fecha

query_tb_ventas_nf = f"""
SELECT *
FROM neg_fijo.tb_ventas_nf
WHERE CAST(fecha_estado AS DATE) >= '{fecha_inicio_str}'
  AND CAST(fecha_estado AS DATE) <= '{fecha_fin_str}';
"""
df_tb_ventas_nf = pd.read_sql(query_tb_ventas_nf, conn_inteligencia_comercial)

# Cerrar las conexiones
conn_inteligencia_comercial.close()
conn_db_inteligencia_comercial.close()
conn_db_inteligencia_comercial_desarrollo.close()


# %%
display(df_tb_ventas_nf.head(3))

# %%
display(df_tb_calendario_temp.head(3))

# %%
columnas_a_eliminar = [
    'sector_economico', 'trm', 'llave;id_venta;ot;enlace;anio_gestion;mes_gestion;tipo_v;fecha_'
]

# Verifica qué columnas existen en el DataFrame
columnas_presentes = [col for col in columnas_a_eliminar if col in df_tb_ventas_nf.columns]

# Elimina solo las columnas que existen
df_tb_ventas_nf = df_tb_ventas_nf.drop(columns=columnas_presentes)

# Mostrar las primeras filas del DataFrame resultante
display(df_tb_ventas_nf.head(2))


# %%


# %%
# Verificar cuántos registros tienen 'NP' en la columna 'red'
num_registros_np = (df_tb_ventas_nf['red'] == 'NP').sum()
print(f"🔍 Registros con 'NP' en la columna 'red' antes de eliminar: {num_registros_np}")

# Eliminar las filas donde 'red' sea 'NP'
df_tb_ventas_nf = df_tb_ventas_nf[df_tb_ventas_nf['red'] != 'NP']

# Verificar que los registros han sido eliminados
num_registros_np_despues = (df_tb_ventas_nf['red'] == 'NP').sum()
print(f"✅ Registros con 'NP' en la columna 'red' después de eliminar: {num_registros_np_despues}")

# Mostrar las primeras filas del DataFrame después de la eliminación
display(df_tb_ventas_nf.head(3))


# %%
# Verificar cuántos registros tienen 'NP' en la columna 'red'
num_registros_NEG_P = (df_tb_ventas_nf['tipo_v'] == 'NEG_P').sum()
print(f"🔍 Registros con 'NP' en la columna 'red' antes de eliminar: {num_registros_NEG_P}")

# Eliminar las filas donde 'tipo_v' sea 'NP'
df_tb_ventas_nf = df_tb_ventas_nf[df_tb_ventas_nf['tipo_v'] != 'NEG_P']

# Verificar que los registros han sido eliminados
num_registros_NEG_P_despues = (df_tb_ventas_nf['tipo_v'] == 'NEG_P').sum()
print(f"✅ Registros con 'NP' en la columna 'tipo_v' después de eliminar: {num_registros_NEG_P_despues}")

# Mostrar las primeras filas del DataFrame después de la eliminación
display(df_tb_ventas_nf.head(3))

# %%
def eliminar_registros_neg_p(df, columnas_objetivo):
    try:
        for col in columnas_objetivo:
            num_antes = (df[col] == 'NEG_P').sum()
            print(f"🔍 Registros con 'NEG_P' en '{col}' antes de eliminar: {num_antes}")

        # Filtra filas donde **ninguna** de las columnas tenga 'NEG_P'
        for col in columnas_objetivo:
            df = df[df[col] != 'NEG_P']

        for col in columnas_objetivo:
            num_despues = (df[col] == 'NEG_P').sum()
            print(f"✅ Registros con 'NEG_P' en '{col}' después de eliminar: {num_despues}")

        return df

    except Exception as e:
        print(f"❌ Error eliminando registros con 'NEG_P': {e}")
        return df

# %%
columnas_a_limpiar = ['tipo_venta', 'ito_mx', 'num_contrato', 'estado_contrato']
df_tb_ventas_nf = eliminar_registros_neg_p(df_tb_ventas_nf, columnas_a_limpiar)

# Visualiza el resultado
display(df_tb_ventas_nf.head(3))

# %%
# Verificar cuántos registros tienen 'NEG_P' en la columna 'tipo_venta'
num_registros_NEG_P = (df_tb_ventas_nf['tipo_venta'] == 'NEG_P').sum()
print(f"🔍 Registros con 'NEG_P' en la columna 'tipo_venta' antes de eliminar: {num_registros_NEG_P}")

# Eliminar las filas donde 'tipo_venta' sea 'NEG_P'
df_tb_ventas_nf = df_tb_ventas_nf[df_tb_ventas_nf['tipo_venta'] != 'NEG_P']

# Verificar que los registros han sido eliminados
num_registros_NEG_P_despues = (df_tb_ventas_nf['tipo_venta'] == 'NEG_P').sum()
print(f"✅ Registros con 'NEG_P' en la columna 'tipo_venta' después de eliminar: {num_registros_NEG_P_despues}")

# Mostrar las primeras filas del DataFrame después de la eliminación
display(df_tb_ventas_nf.head(3))


# %%
# Verificar cuántos registros tienen 'NP' en la columna 'red'
num_registros_NEG_P = (df_tb_ventas_nf['tipo_v'] == 'NEG_P').sum()
print(f"🔍 Registros con 'NP' en la columna 'red' antes de eliminar: {num_registros_NEG_P}")

# Eliminar las filas donde 'tipo_v' sea 'NP'
df_tb_ventas_nf = df_tb_ventas_nf[df_tb_ventas_nf['tipo_v'] != 'NEG_P']

# Verificar que los registros han sido eliminados
num_registros_NEG_P_despues = (df_tb_ventas_nf['tipo_v'] == 'NEG_P').sum()
print(f"✅ Registros con 'NP' en la columna 'tipo_v' después de eliminar: {num_registros_NEG_P_despues}")

# Mostrar las primeras filas del DataFrame después de la eliminación
display(df_tb_ventas_nf.head(3))

# %%
# Mostrar los valores únicos del campo 'jefe'
valores_unicos_jefe = df_tb_ventas_nf['jefe'].unique()
print(valores_unicos_jefe)

# %%
def normalizar_jefes(df):
    try:
        valores_a_reemplazar = ['SIN ASIGNACION', 'VENTAS SIN ASIGNAR', 'NO APLICA']
        df['jefe'] = df['jefe'].replace(valores_a_reemplazar, 'OTROS')
        print("✅ Reemplazo exitoso.")
    except Exception as e:
        print(f"❌ Error al reemplazar valores: {e}")


# %%
normalizar_jefes(df_tb_ventas_nf)

# Verifica el resultado
print(df_tb_ventas_nf['jefe'].unique())


# %%
def corregir_caracteres_malos(df):
    try:
        reemplazos = {
            '�': 'ñ',
            'Ñ': 'Ñ',  # puedes agregar más si lo necesitas
        }

        for col in df.select_dtypes(include=['object', 'string']).columns:
            if df[col].apply(lambda x: isinstance(x, str)).any():
                for malo, bueno in reemplazos.items():
                    df[col] = df[col].astype(str).str.replace(malo, bueno, regex=False)

        print("✅ Caracteres corregidos exitosamente.")
    except Exception as e:
        print(f"❌ Error al corregir caracteres: {e}")



# %%
corregir_caracteres_malos(df_tb_ventas_nf)

# Verifica que ya no haya caracteres extraños
print(df_tb_ventas_nf['jefe'].unique())

# %%
# Mostrar los valores únicos del campo 'linea_1_pyme'
valores_unicos_producto = df_tb_ventas_nf['linea_1_pyme'].unique()
print(valores_unicos_producto)

# %%
def reemplazar_no_registra(df, columna):
    try:
        df[columna] = df[columna].replace('NO REGISTRA', 'OTROS')
        print(f"✅ Valores 'NO REGISTRA' reemplazados por 'OTROS' en la columna '{columna}'.")
    except Exception as e:
        print(f"❌ Error al reemplazar en la columna '{columna}': {e}")


# %%
reemplazar_no_registra(df_tb_ventas_nf, 'linea_1_pyme')

# Verifica los valores únicos luego del reemplazo
print(df_tb_ventas_nf['linea_1_pyme'].unique())

# %%
# Mostrar los valores únicos del campo 'coordinador'
valores_unicos_coordinador = df_tb_ventas_nf['coordinador'].unique()
print(valores_unicos_coordinador)

# %%
def reemplazar_valor(df, columna, valor_antiguo, valor_nuevo):
    try:
        df[columna] = df[columna].replace(valor_antiguo, valor_nuevo)
        print(f"✅ '{valor_antiguo}' fue reemplazado por '{valor_nuevo}' en la columna '{columna}'.")
    except Exception as e:
        print(f"❌ Error al reemplazar valores en '{columna}': {e}")


# %%
reemplazar_valor(df_tb_ventas_nf, 'coordinador', 'NO APLICA', 'OTROS')

# Verifica resultado
print(df_tb_ventas_nf['coordinador'].unique())

# %%
def estandarizar_gerencia(df):
    try:
        import re
        
        # Normalización previa: quitar espacios, saltos de línea y convertir a string
        df['gerencia'] = df['gerencia'].astype(str).str.strip().str.replace(r'[\n\r\t]+', '', regex=True)

        # Lista de variantes problemáticas
        valores_a_reemplazar = [
            'NO APLICA', 'NO REGISTRA', 'OTRO', 'OTRA',
            'SIN ASIGNACION', 'SIN ASIGNACIÓN', 'SIN ASIGNACÍON',
            'SIN ASIGNAción', 'SIN ASIGNACI�N', 'SIN ASIGNACI�@N',
            'SIN ASIGNACIÓN\n', 'SIN ASIGNACION\n', 'SIN ASIGNACI�N\n',
            'SIN ASIGNACI?N', 'SIN ASIGNACI?N\n', 'SIN ASIGNACIￜN',
            'SIN ASIGNACIñN', 'SIN ASIGNACIï¿œN',
            'SIN ASIGNACI”N'  # variante extra por si acaso
        ]

        df['gerencia'] = df['gerencia'].replace(valores_a_reemplazar, 'OTROS')

        print("✅ Campo 'gerencia' estandarizado correctamente.")
    except Exception as e:
        print(f"❌ Error al estandarizar 'gerencia': {e}")


# %%
estandarizar_gerencia(df_tb_ventas_nf)

# Verifica los resultados únicos
print(df_tb_ventas_nf['gerencia'].unique())

# %%
def estandarizar_jefatura(df):
    try:
        df['jefatura'] = df['jefatura'].astype(str).str.strip().str.replace(r'[\n\r\t]+', '', regex=True)

        valores_a_reemplazar = [
            'NO APLICA', 'NO REGISTRA', 'OTRO', 'OTRA',
            'SIN ASIGNACION', 'SIN ASIGNACIÓN', 'SIN ASIGNACÍON',
            'SIN ASIGNAción', 'SIN ASIGNACI�N', 'SIN ASIGNACI�@N',
            'SIN ASIGNACIÓN\n', 'SIN ASIGNACION\n', 'SIN ASIGNACI�N\n',
            'SIN ASIGNACI?N', 'SIN ASIGNACI?N\n', 'SIN ASIGNACIￜN',
            'SIN ASIGNACIñN', 'SIN ASIGNACIï¿œN',
            'SIN ASIGNACI”N'
        ]

        df['jefatura'] = df['jefatura'].replace(valores_a_reemplazar, 'OTROS')

        print("✅ Campo 'jefatura' estandarizado correctamente.")
    except Exception as e:
        print(f"❌ Error al estandarizar 'jefatura': {e}")

# %%
estandarizar_jefatura(df_tb_ventas_nf)

# Verifica los valores únicos
print(df_tb_ventas_nf['jefatura'].unique())

# %%
def estandarizar_consultor(df):
    try:
        df['consultor'] = df['consultor'].astype(str).str.strip().str.replace(r'[\n\r\t]+', '', regex=True)

        valores_a_reemplazar = [
            'NO APLICA', 'NO REGISTRA', 'OTRO', 'OTRA',
            'SIN ASIGNACION', 'SIN ASIGNACIÓN', 'SIN ASIGNACÍON',
            'SIN ASIGNAción', 'SIN ASIGNACI�N', 'SIN ASIGNACI�@N',
            'SIN ASIGNACIÓN\n', 'SIN ASIGNACION\n', 'SIN ASIGNACI�N\n',
            'SIN ASIGNACI?N', 'SIN ASIGNACI?N\n', 'SIN ASIGNACIￜN',
            'SIN ASIGNACIñN', 'SIN ASIGNACIï¿œN',
            'SIN ASIGNACI”N'
        ]

        df['consultor'] = df['consultor'].replace(valores_a_reemplazar, 'OTROS')

        print("✅ Campo 'consultor' estandarizado correctamente.")
    except Exception as e:
        print(f"❌ Error al estandarizar 'consultor': {e}")


# %%
estandarizar_consultor(df_tb_ventas_nf)

# Verifica los valores únicos resultantes
print(df_tb_ventas_nf['consultor'].unique())


# %%
# Diccionario para mapear los meses
month_mapping = {
    "ENERO": "01.Enero",
    "FEBRERO": "02.Febrero",
    "MARZO": "03.Marzo",
    "ABRIL": "04.Abril",
    "MAYO": "05.Mayo",
    "JUNIO": "06.Junio",
    "JULIO": "07.Julio",
    "AGOSTO": "08.Agosto",
    "SEPTIEMBRE": "09.Septiembre",
    "OCTUBRE": "10.Octubre",
    "NOVIEMBRE": "11.Noviembre",
    "DICIEMBRE": "12.Diciembre"
}

# Agregar la columna mes_gestion_numero
df_tb_ventas_nf["mes_gestion_numero"] = df_tb_ventas_nf["mes_gestion"].map(month_mapping)

# Mostrar el resultado
display(df_tb_ventas_nf.head(2))

# %%
'''
# Convertir la columna fecha_estado a tipo datetime (sin errors='coerce')
df_tb_ventas_nf['fecha_estado'] = pd.to_datetime(df_tb_ventas_nf['fecha_estado'])

# Convertir la columna Fecha a tipo datetime
df_tb_calendario_temp['Fecha'] = pd.to_datetime(df_tb_calendario_temp['Fecha'])

# Normalizar ambas fechas para ignorar horas, minutos y segundos
df_tb_ventas_nf['fecha_estado'] = df_tb_ventas_nf['fecha_estado'].dt.normalize()
df_tb_calendario_temp['Fecha'] = df_tb_calendario_temp['Fecha'].dt.normalize()

# Realizar el cruce asegurando que solo se traigan las columnas necesarias
df_cruzado = pd.merge(
    df_tb_ventas_nf,
    df_tb_calendario_temp[['Fecha', 'WK_Year', 'WK_Dia', 'Dia_Habil']],
    left_on='fecha_estado',
    right_on='Fecha',
    how='left'
)

# Renombrar columnas seleccionadas
df_cruzado.rename(
    columns={
        'WK_Year': 'semana_anio',
        'WK_Dia': 'dia_semana',
        'Dia_Habil': 'dia_habil'
    },
    inplace=True
)

# Convertir las nuevas columnas a tipo entero con manejo de NaN
df_cruzado['semana_anio'] = df_cruzado['semana_anio'].fillna(0).astype('Int64')
df_cruzado['dia_semana'] = df_cruzado['dia_semana'].fillna(0).astype('Int64')
df_cruzado['dia_habil'] = df_cruzado['dia_habil'].fillna(0).astype('Int64')

# Seleccionar las columnas relevantes para actualizar el DataFrame original
columnas_adicionales = ['semana_anio', 'dia_semana', 'dia_habil']
df_tb_ventas_nf[columnas_adicionales] = df_cruzado[columnas_adicionales]

# Mostrar el DataFrame actualizado
display(df_tb_ventas_nf.head(5))
'''


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
    
   
    return str(uuid.uuid4())

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

        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_desarrollo}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'control_procesamiento'
        nombre_tabla = 'tb_resumen_cargue'
        
        df_resumen_cargue.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)

    finally:
        conexion.dispose()

# %%
def cargueDatosBD(df_tb_ventas_nf):
    """
    Función que se encarga de cargar los dataframes procesados hacia la base de datos
    
    Argumentos:
        df_tb_ventas_nf: Contiene el dataframe que se requiere cargar a la BD
    Retorna: 
        None
    Excepciones manejadas: 
        SQLAlchemyError as e: Captura el error en caso de que no se puedan insertar los datos en BD y genera un log localmente
    """
    try:
        
        conexion = create_engine(f'postgresql://{par.usuario}:{par.contrasena}@{par.host}:{par.port}/{par.bd_inteligencia_comercial_desarrollo}')
        # Especificar el esquema y la tabla en la que deseas insertar los datos
        nombre_esquema = 'procesamiento_negocios'
        nombre_tabla = 'tb_negocios_fijo_ventas'
        
        df_tb_ventas_nf.to_sql(nombre_tabla, con=conexion, schema=nombre_esquema, if_exists='append', index=False)
        
    except SQLAlchemyError as e:
        logging.error(f"Error SQLAlchemyError: {e}")
    finally:
        conexion.dispose()

# %%
# Generar un UUID único para la ejec
id_ejecucion = str(uuid.uuid4())

# Función para agregar campos de control al DataFrame
def agregar_campos_control(df):
    df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]  # UUID único por fila
    df['id_ejecucion'] = id_ejecucion  # ID único de ejecución
    df['fecha_procesamiento'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Fecha actual
    df['id_estado'] = 1  # Estado inicial
    return df

# Aplicar la función al DataFrame real
df_tb_ventas_nf = agregar_campos_control(df_tb_ventas_nf)

# Llenar columnas nulas con valores predeterminados
valores_por_defecto = {
    'id_estado': 1, 
    'fecha_legalizacion': '1900-01-01',  # Fecha por defecto para campos de fecha
    'direccion': 'NO REGISTRA',  # Texto predeterminado para campos de texto
    # Agregar más columnas y valores predeterminados según sea necesario
}
df_tb_ventas_nf.fillna(value=valores_por_defecto, inplace=True)

# Mostrar el DataFrame actualizado
print("DataFrame actualizado con campos de control y sin nulos:")
display(df_tb_ventas_nf.head(2))


# %%
destino_negocios_fijo  = 'tb_negocios_fijo_ventas'
df_importado = df_tb_ventas_nf
#id_ejecucion = 'da584d92-79bf-470c-82c7-05c02f9c423a'
fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
fecha_inicio_date = datetime.strptime(fecha_inicio, "%Y-%m-%d %H:%M:%S")
registros = len(df_importado)

# %%
df_importado.count()

# %%
cargueDatosBD(df_importado)

# %%



