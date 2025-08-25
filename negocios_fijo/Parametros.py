'''
PROYECTO:		    EMPRESAS Y NEGOCIOS
AUTOR:			    HITSS BI - JORGE MOSQUERA
OPERACIÓN: 		    MÓDULO CONTINE LAS CONSULTAS SQL,DICCIONARIOS PARA EXTRACCIONES PROCESO NEGOCIOS FIJO
VERSIÓN:            V_1.0
FECHA:              15/07/2025
DESCRIPCIÓN:	    CONSULTAS SQL PLANTA COMERCIAL Y DICCIONARIOS.
'''

# Diccionario con la ruta de los modulos principales de conexion a BD

ruta_modulos = {
    "ruta": r"C:\Git_Empresas_Negocios\GlobalHitss-Automation\config",
    "rut_cl": r"C:\Git_Empresas_Negocios\GlobalHitss-Automation\utils"
}

########################################## Proceso planta comercial ########################################

# Dicionario que contine las cunsultas sql de la planta comercia
Planta_Comercial = {
        # Consulta sql a tabla parametrica que controla rutas de archivos, nombre de pestañas a cargar de los archivos.
        "tb_aux_imp":"select * from proc_genericos.tb_aux_importacion_bases where id_base in(13,33,34,35,36)", 
        }
#### Pestanna red maestra

# Diccionario clave valor para porceso pestanna red maestra, usado para filtar por campo id_base
columnas_id = {
"base_pc_retail":33,
"base_pc_directos":34,
"base_pc_cavs_tiendas":35,
"base_pc_retiros":36,
"base_planta_comercial":13
}

# Diccionario de columnas, usado en la funcion de transformacion pestanna redmaestra
columnas_trsf_redmaestrar = {
    # pestanna red maestra
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

# Diccionario de columnas, usado en la funcion de transformacion pestanna retail
columnas_trsf_retail ={
 # pestanna RETAIL
    'NIT':('nit',False),
    'NOMBRE':('nombre_retail',False),
    'GERENTE':('gerente',False),
    'ESPECIALISTA':('especialista',False),
    'CIUDAD':('ciudad',False),

}
