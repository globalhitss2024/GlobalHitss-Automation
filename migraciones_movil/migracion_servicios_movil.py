"""
PROYECTO:   EMPRESAS Y NEGOCIOS
AUTOR:      HITSS BI - GEORGE GALINDO
OPERACIÓN:  CARGUE TABLA movil.tb_servicios_movil DESDE EXCEL
VERSIÓN:    V_1.0
FECHA:      11/08/2025
"""

# %%
import os, sys, uuid
import pandas as pd
from datetime import datetime
sys.path.append(r"C:\Users\46196682\Documents\Automatizacion\GlobalHitss-Automation")

from config.config import conIntelienciaComercial
from utils.Class_read_trsf_excel import load_update_Datos

# --- Parámetros ---
RUTA_EXCEL = r"C:\Users\46196682\OneDrive - Comunicacion Celular S.A.- Comcel S.A\BasesMantenimiento - bases\Servicios_Movil.xlsx"
HOJA = "Servicios_Movil"
TABLA = "tb_servicios_movil"
SCHEMA = "sch_neg_movil"

# %%
def cargar_servicios_movil(path_excel: str, hoja: str) -> pd.DataFrame:
    df = pd.read_excel(path_excel, sheet_name=hoja)
    if "Producto" not in df.columns:
        raise ValueError("La columna obligatoria 'Producto' no está en el archivo.")
    # normalizar nombres y de-duplicar
    df.columns = [c.replace(" ", "_").lower() for c in df.columns]
    claves = ['producto', 'familia_de_productos', 'servicio_homologado', 'servicio', 'gestion_gerente']
    faltan = [c for c in claves if c not in df.columns]
    if faltan:
        raise ValueError(f"Faltan columnas clave: {faltan}")
    return df.drop_duplicates(subset=claves)

def agregar_columnas_control(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    id_ejec = str(uuid.uuid4()).upper()
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["id_ejecucion"] = id_ejec
    df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]
    df["fecha_procesamiento"] = fecha
    df["id_estado_registro"] = 1
    return df

# %%
if __name__ == "__main__":

    dev_conn = conIntelienciaComercial()
    dev_conn.db = "DBInteligenciaComercialDesarrollo"  # Cambia la BD
    engine = dev_conn.get_postgres_connect()

    try:
        df = cargar_servicios_movil(RUTA_EXCEL, HOJA)
        df = agregar_columnas_control(df)

        cargador = load_update_Datos(engine_conexion=engine)
        cargador.cargar_df_a_tabla(df_tabla=df, name=TABLA, schema=SCHEMA)

        print(f"Carga OK -> {SCHEMA}.{TABLA} | Filas: {len(df)}")

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