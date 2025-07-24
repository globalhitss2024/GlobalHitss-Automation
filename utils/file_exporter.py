'''
PROYECTO:		    ANALÍTICA DE EMPRESAS Y NEGOCIOS
FRENTE DE TRABAJO:	DESARROLLO TRANSVERSAL
AUTOR:			    HITSS - FERNANDA ZAMBRANO
OPERACIÓN: 		    UTIL QUE PERMITE EXPORTAR DATAFRAMES
VERSIÓN:	        V_1.0
FECHA:              23/07/2025
DESCRIPCIÓN:	    SCRIPT QUE PERMITE EXPORTAR DATAFRAMES A FORMATO XLSX
'''

import os
from datetime import datetime
import pandas as pd

#Exportar DataFrames de pandas a archivos Excel

#Asegurarse de que el directorio de salida exista, si no, lo crea
class FileExporter:
    def export_to_excel(self, df: pd.DataFrame, filename_prefix: str, path: str):
        if not os.path.exists(path):
            os.makedirs(path)
            print(f"Directorio creado: {path}")

# Crea la ruta completa del archivo con el prefijo de nombre y la fecha actual
        today_str = datetime.now().strftime("%Y%m%d")
        filename = f"{filename_prefix}_{today_str}.xlsx"
        full_path = os.path.join(path, filename)

# Exporta el DataFrame a un archivo Excel
        df.to_excel(full_path, index=False)
        print(f"Datos exportados correctamente a: {full_path}")
        return full_path