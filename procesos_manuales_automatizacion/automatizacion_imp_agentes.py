import pandas as pd
import sys
import os

# Configurar ruta del proyecto
ruta_proyecto = r'C:\01_Inteligencia_Comercial'

if ruta_proyecto not in sys.path:
    sys.path.append(ruta_proyecto)

from utils.Utils_auto_agentes import DataUtils
utils = DataUtils()

def cargar_datos_excel(ruta):
    df_fijo = pd.read_excel(ruta, sheet_name='Base Empresas Fija (2)')
    df_movil = pd.read_excel(ruta, sheet_name='Base Movil')
    return df_fijo, df_movil

def limpiar_fijo(df):
    df['CONSULTOR'] = df['CONSULTOR'].apply(utils.limpiar_texto)
    df['PRODUCTO_LIMPIO_BASE'] = df['PRODUCTO'].apply(utils.limpiar_texto)
    df['producto_limpio'] = df['PRODUCTO_LIMPIO_BASE'].apply(utils.clasificar_producto)
    df['TOTAL VENTAS'] = df['TOTAL VENTAS'].fillna(0).astype(int)
    df['COMISIONES'] = df['COMISIONES'].fillna(0).astype(int)
    df['FECHA'] = pd.to_datetime(df['FECHA'])
    df['MES_AÑO'] = df['MES'].dt.strftime('%m-%Y')
    return df

def limpiar_movil(df_movil):
    df_movil['CFM$'] = df_movil['CFM$'].apply(utils.parse_moneda)
    df_movil['LINEAS'] = pd.to_numeric(df_movil['LINEAS'], errors='coerce').fillna(0).astype(int)
    df_movil['MES_AÑO'] = df_movil['MES'].dt.strftime('%m-%Y')
    df_movil['NOMBRE CONSULTOR'] = df_movil['NOMBRE CONSULTOR'].apply(utils.limpiar_texto)
    df_movil['CEDULA_CONS'] = pd.to_numeric(df_movil['CEDULA_CONS'], errors='coerce').astype('Int64')
    mapeo_donante = {'MOVISTAR': 'MOVISTAR', 'TIGO': 'TIGO', 'WOM': 'WOM', 'AVANTEL': 'AVANTEL'}
    df_movil['donante_receptor_limpio'] = df_movil['DONANTE_RECEPTOR'].astype(str).str.strip().str.upper().apply(
        lambda x: mapeo_donante.get(x, 'OTROS'))
    return df_movil

def generar_reportes_fijo(df):
    return {
        'Crecimiento Fijo': utils.crear_tabla_resumen(df, ['C.C. CONSULTOR', 'CONSULTOR', 'TIPO'], 'TOTAL VENTAS'),
        'Altas Fijo': utils.crear_tabla_resumen(df[df['TIPO'] == 'Altas'],
                                          ['C.C. CONSULTOR', 'CONSULTOR', 'producto_limpio'], 'TOTAL VENTAS'),
        'Neto Fijo': utils.crear_tabla_resumen(df[df['TIPO'].isin(['Altas', 'Churn', 'Erosión'])],
                                         ['C.C. CONSULTOR', 'CONSULTOR', 'producto_limpio'], 'TOTAL VENTAS'),
        'Cloud Fijo': utils.crear_tabla_resumen(df[(df['PRODUCTO_LIMPIO_BASE'] == 'CLOUD') & (df['TIPO'] == 'Digitadas')],
                                          ['C.C. CONSULTOR', 'CONSULTOR', 'PRODUCTO_LIMPIO_BASE'], 'TOTAL VENTAS')
    }

def generar_reportes_movil(df_movil):
    return {
        'Crecimiento Movil': utils.crear_tabla_resumen(df_movil, ['CEDULA_CONS', 'NOMBRE CONSULTOR', 'TIPO/BASE'], 'CFM$'),
        'Lineas Movil': utils.crear_tabla_resumen(df_movil, ['CEDULA_CONS', 'NOMBRE CONSULTOR', 'TIPO/BASE'], '#  LINEAS'),
        'Port In Movil': utils.crear_tabla_resumen(
            df_movil[df_movil['RAZON'] == 'Activacion Portabilidad Numerica - 267'],
            ['CEDULA_CONS', 'NOMBRE CONSULTOR', 'donante_receptor_limpio'], '#  LINEAS'),
        'Port Out Movil': utils.crear_tabla_resumen(
            df_movil[df_movil['RAZON'] == 'Desactivacion Portabilidad - 248'],
            ['CEDULA_CONS', 'NOMBRE CONSULTOR', 'donante_receptor_limpio'], '#  LINEAS')
    }

def exportar_reportes_excel(ruta_salida, reportes_dict):
    with pd.ExcelWriter(ruta_salida, engine='xlsxwriter') as writer:
        for hoja, df in reportes_dict.items():
            df.to_excel(writer, sheet_name=hoja, index=False)

if __name__ == "__main__":
    utils = DataUtils()
    ruta_excel = f'{ruta_proyecto}\Entradas\Resumen Vtas - Bajas Convergente (6) 1.xlsx'
    ruta_salida = f'{ruta_proyecto}\Salida\resumen_consolidado.xlsx'

    df_fijo, df_movil = cargar_datos_excel(ruta_excel)
    df_fijo = limpiar_fijo(df_fijo)
    df_movil = limpiar_movil(df_movil)

    reportes_fijo = generar_reportes_fijo(df_fijo)
    reportes_movil = generar_reportes_movil(df_movil)

    exportar_reportes_excel(ruta_salida, {**reportes_fijo, **reportes_movil})
