"""
PROYECTO:           ANALÍTICA DE EMPRESAS Y NEGOCIOS
FRENTE DE TRABAJO:  EMPRESAS TRANSVERSAL
AUTOR:              HITSS - FERNANDA ZAMBRANO
OPERACIÓN:          REPORTE QUE PERMITE IDENTIFICAR EL SEGMENTO DEL CLIENTE
VERSIÓN:            V_1.0
FECHA:              23/07/2025
DESCRIPCIÓN:        SCRIPT QUE PERMITE HACER CRUCE ENTRE LAS TABLAS DE ONIX Y ASIGNACIÓN PARA IDENTIFICAR EL SEGMENTO ACTUALIZADO DEL CLLIENTE.
"""

def get_query():
    return """
        SELECT
            a."NIT",
            a."NIT_DV",
            c."iCompanyId"      AS "ID_ONIX",
            a."RAZON_SOCIAL",  
            c."GRUPO_ECONOMICO" AS "GRUPO_OBJETIVO_ONIX",
            a."SEGMENTO_ACTUAL" AS "SEGMENTO_ASIGNACION",
            c."SEGMENTO"        AS "SEGMENTO_ONIX",
            a."ESTADO_CLIENTE_FYM",
            a."CIUDAD",
            a."DEPARTAMENTO",
            c."AddressType"     AS "SEDE",
            a."DIRECCION",
            c."Telefono"        AS "TELEFONO",
            c."Correo"          AS "CORREO_ELECTRONICO",
            LOWER (a."WEB")     AS "WEB",
            UPPER (c."SECTOR")  AS "SIC",
            c."ACTIVIDAD_ECO"   AS "ACTIVIDAD_COMERCIAL",
            a."GO_TO_MARKET"
        FROM
            bd_production.tb_asignacion a
        LEFT JOIN
            public."tbl_Company" c ON a."NIT_DV" = c."NIT"
        WHERE
            a."SEGMENTO_CLIENTE" = 'EMPRESAS'
    """
