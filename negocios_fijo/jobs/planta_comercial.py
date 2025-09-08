'''
PROYECTO:		    EMPRESAS Y NEGOCIOS
AUTOR:			    HITSS BI - FERNANDA ZAMBRANO
OPERACIÓN: 		    PROCESAMIENTO DE ARCHIVO PLANTA COMERCIAL
VERSIÓN:            V_1.0
FECHA:              02/09/2025
DESCRIPCIÓN:	    JOB QUE PERMITE EL PROCESAMIENTO DEL ARCHIVOS DE PLANTA COMERCIAL
'''

import pandas as pd

class red_maestra:
    """
    Contiene la lógica para el ETL de Planta Comercial
    """
    def __init__(self):
        # El mapeo de columnas vive aquí, como parte de la lógica del job.
        self.columnas_a_transformar = {
            'LLAVE': ('llave_comercial', False),
            'CEDULA/NIT': ('identificacion_comercial', False),
            'NOMBRE': ('nombre_comercial', True),
            'CARGO ACTUAL': ('cargo_actual', False),
            'TIPO DE CONTRATACION': ('tipo_contrato', False),
            'ESTADO': ('estado', False),
            'DIRECTOR COMERCIAL': ('director', False),
            'DIRECCION COMERCIAL': ('direccion', False),
            'NOMBRE GERENTE': ('gerente', True),
            'GERENCIA COMERCIAL/ O JEFATURA': ('gerencia', False),
            'JEFE  DIRECTO': ('jefe', True),
            'NOMBRE COORDINADOR DIRECTO': ('coordinador', True),
            'ESPECIALISTA': ('especialista', True),
            'NOMBRE COORDINADOR TERCERO': ('coordinador_tercero', True),
            'SEGMENTO': ('segmento', False),
            'GRUPO COMERCIAL': ('grupo_comercial', False),
            'OPERACIÓN': ('operacion', False),
            'CANAL': ('canal', False),
            'CATEGORIA': ('categoria', False),
            'CATEGORIZACION': ('categorizacion', False),
            'PROVEEDOR': ('proveedor', False),
            'CIUDAD': ('ciudad', False),
            'DEPARTAMENTO': ('departamento', False),
            'REGIONAL': ('regional', False),
        }

    def _transformar_columna(self, df, columna, reemplazar_nbsp=False, valor_nulo='NO REGISTRA'):
        """Función de ayuda para la limpieza estándar de columnas."""
        def limpiar(x):
            if pd.isna(x) or str(x).strip() == '':
                return valor_nulo
            texto = str(x).strip()
            if reemplazar_nbsp:
                texto = texto.replace('&amp;amp;nbsp;', '')
            return texto.upper()
        return df[columna].apply(limpiar)

    def run_transformacion(self, df_input):
        """La función principal que ejecuta todos los pasos de la transformación."""
        print("Ejecutando transformaciones para Planta Comercial...")
        df_transformado = pd.DataFrame()

        # Lógica de transformación principal
        df_transformado['id_planta_comercial'] = df_input['Consecutivo']
        
        for original, (nuevo, reemplazar_nbsp) in self.columnas_a_transformar.items():
            df_transformado[nuevo] = self._transformar_columna(df_input, original, reemplazar_nbsp)
        
        df_transformado['fecha_ingreso'] = pd.to_datetime(
            df_input['FECHA DE INGRESO A LA COMPAÑIA'], errors='coerce'
        ).fillna(pd.Timestamp('1900-01-01'))
        
        df_transformado['codigo_rr'] = df_input['CODIGO RR O HFC'].apply(
            lambda x: 'PENDIENTE' if pd.isna(x) or str(x).strip() == '' else str(x).strip().upper()
        )
        
        # Filtro de calidad
        df_transformado = df_transformado[
            df_input['Consecutivo'].notna() &
            df_input['CEDULA/NIT'].apply(lambda x: not (pd.isna(x) or str(x).strip() == ''))
        ]
        
        print(f"Transformación finalizada. {len(df_transformado)} registros procesados.")
        return df_transformado
