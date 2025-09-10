"""
PROYECTO:		    EMPRESAS Y NEGOCIOS
AUTOR:			    HITSS BI - FERNANDA ZAMBRANO
OPERACIÓN: 		    SCRIPT ASOCIADO A NEGOCIOS FIJO
VERSIÓN:            V_1.0
FECHA:              02/09/2025
DESCRIPCIÓN:	    SCRIPT QUE PERMITE CONSULTAR LO CORRESPONDIENTE A NEGOCIOS FIJO
"""

# Planta Comercial
def get_query():
    return """
        SELECT
            a.id_base,
            a.nombre_base,
            a.importar,
            a.fecha_importacion,
            a.nombre_archivo_fuente,
            a.fecha_modificacion_archivo,
            a.nombre_corto_base,
            a.tipo_transfer,
            a.rango,
            a.nombre_tbl_servidor,
            a.nombre_ct_insercion,
            a.nombre_esquema_servidor
        FROM
            proc_genericos.tb_aux_importacion_bases a
        WHERE
            a.id_base in (13, 33, 34, 35, 36);
    """