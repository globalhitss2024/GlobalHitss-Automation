"""
PROYECTO:   EMPRESAS Y NEGOCIOS
AUTOR:      HITSS BI - GEORGE GALINDO
OPERACIÓN:  CARGUE TABLA sch_neg_movil.tb_causales_ DESDE EXCEL
VERSIÓN:    V_1.0
FECHA:      12/08/2025
"""

# %%
import os, sys, uuid, re, unicodedata
import pandas as pd
from datetime import datetime

# Ruta del repo/proyecto
sys.path.append(r"C:\Users\46196682\Documents\Automatizacion\GlobalHitss-Automation")

from config.config import conIntelienciaComercial
from utils.Class_read_trsf_excel import load_update_Datos

# --- Parámetros ---
RUTA_EXCEL = r"C:\Users\46196682\OneDrive - Comunicacion Celular S.A.- Comcel S.A\BasesMantenimiento - bases\Causales.xlsx"
HOJA = "Causales"
SCHEMA = "sch_neg_movil"
TABLA  = "tb_causales_movil"


# --- ETL Causales ---
def cargar_causales(path_excel: str, hoja: str) -> pd.DataFrame:
    """
    Lee la hoja 'Causales', valida columna clave, normaliza nombres y de-dup por llaves.
    """
    df = pd.read_excel(path_excel, sheet_name=hoja)

    # Validación mínima
    if "RAZON_INICIAL" not in df.columns:
        raise ValueError("La columna obligatoria 'RAZON_INICIAL' no está en el Excel.")

    # Normaliza nombres de columnas
    df.columns = [c.replace(" ", "_").lower() for c in df.columns]

    # Llaves para duplicados
    claves = ['razon_inicial', 'tipo', 'tipo_v', 'tipo_g', 'td']
    faltan = [c for c in claves if c not in df.columns]
    if faltan:
        raise ValueError(f"Faltan columnas clave en Excel: {faltan}")

    # De-dup en origen
    df = df.drop_duplicates(subset=claves)
    return df

def agregar_columnas_control(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    id_ejec = str(uuid.uuid4()).upper()
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["id_ejecucion"] = id_ejec
    df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]
    df["fecha_procesamiento"] = fecha
    df["id_estado_registro"] = 1
    return df

def construir_llave(df: pd.DataFrame) -> pd.DataFrame:
    """Concatena las columnas clave para anti-join contra histórico."""
    df = df.copy()
    df["llaveDupli"] = (
        df["razon_inicial"].astype(str)
        + df["tipo"].astype(str)
        + df["tipo_v"].astype(str)
        + df["tipo_g"].astype(str)
        + df["td"].astype(str)
    )
    return df


# %%
if __name__ == "__main__":
    # Conexión: reusar clase y solo cambiar nombre de BD si necesitas DEV
    dev_conn = conIntelienciaComercial()
    dev_conn.db = "DBInteligenciaComercialDesarrollo"
    engine = dev_conn.get_postgres_connect()

    try:
        #  Excel - DF limpio
        df = cargar_causales(RUTA_EXCEL, HOJA)
        df = construir_llave(df)

        cargador = load_update_Datos(engine_conexion=engine)
        cargador.cargar_df_a_tabla(df_tabla=df, name=TABLA, schema=SCHEMA)

        print(f"Carga OK -> {SCHEMA}.{TABLA} | Filas nuevas: {len(df)}")

    except Exception as e:
        print("Error en la carga:", e)

        try:
            conn = engine.raw_connection()
            if conn and conn.in_transaction():
                conn.rollback()
                print("Rollback ejecutado.")
            if conn:
                conn.close()
        except Exception as rb_err:
            print("Error durante rollback:", rb_err)
            