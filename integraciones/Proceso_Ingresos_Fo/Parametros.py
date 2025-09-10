"""
PROYECTO:           EMPRESAS Y NEGOCIOS
AUTOR:              HITSS BI - Johana Perez Montoya
OPERACIÓN:          MÓDULO CONTIENE LAS CONSULTAS SQL, DICCIONARIOS PARA EXTRACCIONES PROCESO NEGOCIOS FIJO
VERSIÓN:            V_1.0
FECHA:              06/08/2025
DESCRIPCIÓN:        CONSULTAS SQL INGRESOS FO y diccionarios.
"""

import pandas as pd

ruta_modulos = {
    "ruta": r"C:\GIT_Empresas_Negocios\GlobalHitss-Automation\config",
    "rut_cl": r"C:\Git_Empresas_Negocios\GlobalHitss-Automation\utils",
}

# =======================
# RUTAS PARA EL PROCESO
# =======================

ruta_ciclos = r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\CICLOS_202507.xlsx"
ruta_ri = r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\RI_202507 (1).xlsb"
ruta_salida = r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\Exportaciones"
ruta_homologacion = r"C:\Users\perezjomi\Documents\Automatización_Ingresos_FO_HFC\Tabla Homologación UMC.xlsx"

# =======================
# Diccionario de columnas RI
# =======================

dic_columnas_ri = {
    "columnas_deseadas": [
        "rm_periodo",
        "Enlace",
        "IDOnyxFacturador",
        "ID Onyx Propietario",
        "Nombre Cliente Facturador",
        "Familia",
        "Producto",
        "Servicio",
        "Segmento Mercado",
        "Clasificacion Segmento",
        "Tipo",
        "Cargo Mensual MO Pesos",
        "Arrendamiento MO Pesos",
        "Rec Ini MO COP",
        "Cargo Mensual MO USD",
        "Arrendamiento MO USD",
        "Rec Ini MO USD",
        "TRM",
        "Recurrente Inicial Pesos",
        "Movimiento Cargo Mensual MO Pesos",
        "Movimiento Arrendamiento MO Pesos",
        "Movimiento Total en Pesos",
        "Cargos One Time",
    ],
}

# =======================
# Diccionario de columnas CICLOS
# =======================

dic_columnas_ciclos = {
    "rename": {
        "id cliente": "IDOnyxFacturador",
        "nit": "NIT_DV",
    },
    "columnas_deseadas": [
        "IDOnyxFacturador",
        "NIT_DV",
    ],
}

# =======================
# Diccionario de ORDEN FINAL
# =======================

dic_orden_columnas = {
    "orden": [
        "rm_periodo",
        "Enlace",
        "IDOnyxFacturador",
        "ID Onyx Propietario",
        "Nombre Cliente Propietario",
        "Nombre Cliente Facturador",
        "CONTENADO_ID",
        "Familia",
        "Producto",
        "Servicio",
        "Segmento Mercado",
        "Clasificacion Segmento",
        "Tipo",
        "Cargo Mensual MO Pesos",
        "Arrendamiento MO Pesos",
        "Rec Ini MO COP",
        "Recurrente Inicial Pesos",
        "Rec Ini MO USD",
        "TRM",
        "Movimiento Cargo Mensual MO Pesos",
        "Movimiento Arrendamiento MO Pesos",
        "Movimiento MO COP",
        "Movimiento Total en Pesos",
        "Cargos One Time",
        "NIT",
        "Nombre cliente",
        "ciclo",
        "NIT_DV",
        "RAZON_SOCIAL",
        "SEGMENTO_ACTUAL",
        "GERENCIA_ACTUAL_FIJO",
        "GERENTE_ACTUAL_FIJO",
        "Director",
        "Cedula Director",
        "Coordinador",
        "Cedula Coordinador",
        "JEFE ACTUAL FIJO",
        "DOCUMENTO_JEFE",
        "DIRECCION_COMERCIAL",
        "CONSULTOR_ACTUAL_FIJO",
        "CEDULA_CONSULTOR_FIJO",
        "Flia Final Col (Rent)V2",
        "Flia AMX_V2",
    ],
}

# =======================
# Diccionario de columnas en Pesos
# =======================

dic_columnas_pesos = {
    "columnas_pesos": [
        "Cargo Mensual MO Pesos",
        "Arrendamiento MO Pesos",
        "Rec Ini MO COP",
        "Recurrente Inicial Pesos",
        "Movimiento Cargo Mensual MO Pesos",
        "Movimiento Arrendamiento MO Pesos",
        "Movimiento Total en Pesos",
        "Cargos One Time",
    ],
}
