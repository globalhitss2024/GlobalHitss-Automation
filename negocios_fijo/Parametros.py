'''
PROYECTO:       EMPRESAS Y NEGOCIOS
AUTOR:          HITSS BI - FERNANDA ZAMBRANO
OPERACIÓN:      PARÁMETROS CONSOLIDADOS PARA NEGOCIOS FIJO
VERSIÓN:        V_1.0
FECHA:          02/09/2025
DESCRIPCIÓN:    CONFIGURACIÓN DE PARÁMETROS TRANSVERSALES PARA TODOS LOS PROCESOS DE NEGOCIOS FIJO
'''
# Configuración de rutas
ruta_modulos = {
    "ruta": r"C:\Users\46120442\OneDrive - GLOBAL HITSS\Documentos\Proyectos Empresas y Negocios\HU0019\Desarrollo\config",
    "rut_cl": r"C:\Users\46120442\OneDrive - GLOBAL HITSS\Documentos\Proyectos Empresas y Negocios\HU0019\Desarrollo\utils"
}

# IDs que la clase procesador_base necesita para encontrar la configuración en la tabla aux de BD
columnas_id = {
    "base_planta_comercial": 13,
    "base_pc_retail": 33,
    "base_pc_directos": 34,
    "base_pc_cavs_tiendas": 35,
    "base_pc_retiros": 36
}