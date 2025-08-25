'''
PROYECTO:		    EMPRESAS Y NEGOCIOS
AUTOR:			    HITSS BI - JORGE MOSQUERA
OPERACION: 		    MODULO CONTINE DICCIONARIOS CON CONFIGURACIONES Y DATOS REQUERIDOS PARA EL PROCESO PBI TRAFICO CAVS
VERSION:            V_1.0
FECHA:              31/07/2025
DESCRIPCION:	    DICCIONARIOS CON CONFIGURACIONES Y DATOS REQUERIDOS PARA EL PROCESO PBI TRAFICO CAVS.
'''

# Diccionario con la ruta de los modulos principales de conexion a BD

ruta_modulos = {
"ruta": r"C:\01_Inteligencia_Comercial\config",
"rut_cl": r"C:\01_Inteligencia_Comercial\utils",
"rut_con_sql": r"C:\01_Inteligencia_Comercial\sql\pbi_trafico_cavs"
}

tablas_eschema={
"eschema":"public",
"h_digi":"tb_historico_digiturno",
"fij_mov":"tb_historico_serv_lineas",
"asig_cav":"tb_hist_asig_pc_cav"
}
# Separador usado para leer los archivos csv
#separador={";"}
# orden columnas cavs

# dicoinario reordenamiento campos cavas
cavs_org = {
    'fecha': 1,
    'nom_cav': 2,
    'cvc': 3,
    'gerente': 4,
    'jefe': 5,
    'especialista': 6,
    'cedula_gerente': 7,
    'cedula_jefe': 8,
    'cedula_especialista': 9,
    'mes':10,
    'anio':11,
    'llave_cvc_mes_anio':12,
    'fec_carga': 13
}

# diccionario calulo meses
meses_es_dict = {
    1: "ENERO",
    2: "FEBREO",
    3: "MARZO",
    4: "ABRIL",
    5: "MAYO",
    6: "JUNIO",
    7: "JULIO",
    8: "AGOSTO",
    9: "SEPTIEMBRE",
    10: "OCTUBRE",
    11: "NOVIEMBRE",
    12: "DICIEMBRE"
}

# Codificacion usada para leer los archivos csv
#codificacion={"utf-8"}

