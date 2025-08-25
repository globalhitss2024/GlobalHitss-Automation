# -*- coding: utf-8 -*-
"""
PROYECTO:             EMPRESAS Y NEGOCIOS
AUTOR:                HITSS BI - JORGE MOSQUERA
OPERACION:            SCRIPT EN PYTHON CON LA ETL PARA EL PBI TRAICO CAVS
VERSION:              V_1.0
FECHA:                31/07/2025
DESCRIPCION:          SE CREA SCRIPT CON LAS CONSULTAS SQL PARA EL PROCESO PBI TRAFICO CAVS
"""

# Consulta proceso trafico cavs fija
def query_fija():
        return """
        with TB_FIJA as (
  
  SELECT 
        vt.act_proveedor as NOM_CAV,
        CV."CODIGO PADRE CAV DONDE LABORA" AS CVC,
	vt.mes_gestion as MES,
        RIGHT(vt.anio_gestion::TEXT, 2)::INT as ANIO,
        CV."CODIGO PADRE CAV DONDE LABORA"||'-'||vt.mes_gestion||'-'||RIGHT(vt.anio_gestion::TEXT, 2)::INT as LLAVE_CVC_MES_ANIO,
	vt.tipo_venta as TIPO,
	vt.fecha_estado,
       'FIJO' AS BASE
   FROM neg_fijo.tb_ventas_nf vt
        LEFT JOIN (select distinct 
        "CAVS",
        "CODIGO PADRE CAV DONDE LABORA" 
    	from proc_genericos.tb_crudo_cavs) CV ON vt.act_proveedor = CV."CAVS"
        WHERE anio_gestion='2025' /*YEAR(GETDATE())*/  AND mes_gestion='JULIO' /* DATENAME(MONTH, GETDATE())*/
        AND tipo_v IN('ALTAS_F')
        AND canal='CAV'
        --AND UPPER(vt.act_proveedor) = 'TIENDA PASTO'
        )
select 
	NOM_CAV,
        CVC,
	MES,
        ANIO,
        LLAVE_CVC_MES_ANIO,
	TIPO,
	fecha_estado,
        count(CVC) as CANT_SERV_LINEAS,
	BASE
from TB_FIJA 
        group by NOM_CAV,
                CVC,
	        MES,
                ANIO,
                LLAVE_CVC_MES_ANIO,
	        TIPO,
	        fecha_estado,
                BASE
"""
# Consulta para eliminar los registro del mismo periodo
def delete_query(schema, name):
    return f"""
        DELETE FROM {schema}.{name}
        WHERE mes LIKE :mes_nombre
        AND anio = :anio
    """

def query_movil():
      return '''
with TB_MOVIL as (

select 
vt.proveedor as NOM_CAV,
CV."CODIGO PADRE CAV DONDE LABORA" AS CVC,
vt.mes_gestion as MES,
RIGHT(vt.anio_gestion::TEXT, 2)::INT as ANIO,
CV."CODIGO PADRE CAV DONDE LABORA"||'-'||vt.mes_gestion||'-'||RIGHT(vt.anio_gestion::TEXT, 2)::INT as LLAVE_CVC_MES_ANIO,
vt.tipo as TIPO,
vt.fecha as fecha_estado,
'MOVIL' AS BASE
from movil.tb_ventas_m_v1 vt
LEFT JOIN (select distinct 
        "CAVS",
        "CODIGO PADRE CAV DONDE LABORA" 
    	from proc_genericos.tb_crudo_cavs) CV ON vt.proveedor = CV."CAVS"
WHERE anio_gestion='2025' /*YEAR(GETDATE())*/  AND mes_gestion='JULIO' /* DATENAME(MONTH, GETDATE())*/
        AND tipo IN('ALTAS_M','PREPOS_M')
        AND canal='CAV'
        --AND UPPER(vt.proveedor) = 'CAV CALI PALMETTO'
    	
)        
select 
	NOM_CAV,
        CVC,
	MES,
        ANIO,
        LLAVE_CVC_MES_ANIO,
	TIPO,
	fecha_estado,
        count(CVC) as CANT_SERV_LINEAS,
	BASE
from TB_MOVIL
    group by NOM_CAV,
             CVC,
	     MES,
             ANIO,
             LLAVE_CVC_MES_ANIO,
	     TIPO,
  	     fecha_estado,
             BASE
'''

def query_asig_cav():
      return '''
WITH datos_asig_cav AS (
  SELECT
    "CAVS" AS NOM_CAV,
    "CODIGO PADRE CAV DONDE LABORA" AS CVC,
    "NOMBRE GERENTE" AS GERENTE,
    "NOMBRE JEFE" AS JEFE,
    "NOMBRE ESPECIALISTA" AS ESPECIALISTA,
    "DOCUMENTO GERENTE" AS CEDULA_GERENTE,
    "DOCUMENTO JEFE" AS CEDULA_JEFE,
    "DOCUMENTO ESPECIALISTA" AS CEDULA_ESPECIALISTA,
    ROW_NUMBER() OVER (PARTITION BY "CODIGO PADRE CAV DONDE LABORA" ORDER BY "CAVS") AS rn
  FROM proc_genericos.tb_crudo_cavs
)
SELECT *
FROM datos_asig_cav
WHERE rn = 1;

'''