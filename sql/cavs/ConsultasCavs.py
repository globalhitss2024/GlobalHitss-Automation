"""
PROYECTO:             EMPRESAS Y NEGOCIOS
AUTOR:                HITSS BI - GEORGE GALINDO
OPERACION:            Consulta para la creacion de la tabla de cavs
VERSION:              V_1.0
FECHA:                28/07/2025
DESCRIPCION:          Consulta a tabla neg_fijo.tb_ventas_nf     
"""

from datetime import datetime

# Obtener año actual y anterior
año_actual = datetime.now().year
año_anterior = año_actual - 1


consulta_cavs = f"""
SELECT 
    *
FROM neg_fijo.tb_ventas_nf
WHERE anio_gestion IN ({año_anterior}, {año_actual})
AND CANAL = 'CAV';
"""
