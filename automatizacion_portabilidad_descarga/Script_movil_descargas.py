# %%
import pandas as pd
import pyodbc
import sys
sys.path.append(r"C:\Users\46196682\Documents\Automatizacion\GlobalHitss-Automation")
from sql.movil_portabilidad.Consultas_movil import query_negocios, query_empresas, query_umc, query_movistar_tigo
from datetime import datetime, timedelta

# %%
# Ruta access
ruta_access = r"C:\Users\46196682\Downloads\BD_E&N - Nueva_Estructura (5).accdb"

# Conexion al access
conn_str = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    f"DBQ={ruta_access};"
)
conn = pyodbc.connect(conn_str)

# Convertir Consultas a Dfs
df_negocios = pd.read_sql(query_negocios, conn)
df_empresas = pd.read_sql(query_empresas, conn)
df_umc = pd.read_sql(query_umc, conn)
df_movistar_tigo = pd.read_sql(query_movistar_tigo, conn)

# Cerrar la conexion
conn.close()

# ruta para descarga
nombre_archivo = f"reporte_portabilidad_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
ruta_salida = rf"C:\Users\46196682\Documents\{nombre_archivo}"

# descargar de los dfs
with pd.ExcelWriter(ruta_salida, engine='xlsxwriter') as writer:
    df_negocios.to_excel(writer, sheet_name='NEGOCIOS', index=False)
    df_empresas.to_excel(writer, sheet_name='EMPRESAS', index=False)
    df_umc.to_excel(writer, sheet_name='UMC', index=False)
    df_movistar_tigo.to_excel(writer, sheet_name='MOVISTAR_TIGO', index=False)

print(f"\n Archivo generado correctamente: {ruta_salida}")



# %%
