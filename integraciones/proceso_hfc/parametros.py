"""
===============================================================================
PROYECTO:      EMPRESAS Y NEGOCIOS
MÓDULO:        Parámetros y Diccionarios de Configuración
AUTOR:         Johana Pérez Montoya - HITSS BI
VERSIÓN:       1.0
FECHA:         06/08/2025
-------------------------------------------------------------------------------
DESCRIPCIÓN:
    Este módulo centraliza las rutas de entrada y salida, así como los
    diccionarios de columnas requeridos para el procesamiento de información
    en el marco del Proceso de Negocios Fijo (Ingresos FO y archivos PYM/360).

    Contiene:
      - Definición de rutas de acceso a archivos fuente.
      - Diccionarios de homologación y estructura de columnas esperadas.
      - Parámetros base para los procesos de integración y validación.
-------------------------------------------------------------------------------
DEPENDENCIAS:
    - pandas
-------------------------------------------------------------------------------
MANTENIMIENTO:
    Equipo BI - HITSS
===============================================================================
"""


import pandas as pd

ruta_modulos = {
    "ruta": r"C:\GIT_Empresas_Negocios\GlobalHitss-Automation\config",
    "rut_cl": r"C:\Git_Empresas_Negocios\GlobalHitss-Automation\utils",
}

# =======================
# RUTAS PARA EL PROCESO
# =======================

ruta_ingresos = r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\HFC\07 Julio_2025 Ingresos K-H.csv"
ruta_360 = r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\HFC\07 V360 Julio.CSV"
ruta_pym = r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\HFC\ACTIVO PYM 31.xlsb"
ruta_homologacion = r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\HFC\Tabla Homologación UMC.xlsx"
ruta_salida = r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\HFC\ouput_HFC"
ruta_hfc = r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\HFC\ingresos_hfc.csv"






# parametros.py

dic_columnas_ingresos_hfc = {
    "columnas_deseadas": [
        "CUENTASAP",
        "SEGMENTO",
        "CONCEPTO",
        "SERVICIO",
        "NEGOCIO",
        "AVION",
        "NOMBRE_FAMILIA",
        "TIPO_SERVICIO",
        "VR_SINIVA",
        "TOTAL",
    ]
}


dic_360 = {
    "columnas_deseadas": [
         'NIT_DV',
            'NIT',
            'TIPO_CRM',
            'ID_CLIENTE_CRM',
            'NOMBRE',
            'CIUDAD',
            'SEGMENTO'
    ]
}

dic_columnas_pym = {
"columnas_deseadas": [
    "SUACCT",
    "SUIDT1",
    "NIT SIN ",
    "NOMBRE",
    "SEGMENTO",
    "CIUDAD",
    "DEPARTAMENTO"
]
}


