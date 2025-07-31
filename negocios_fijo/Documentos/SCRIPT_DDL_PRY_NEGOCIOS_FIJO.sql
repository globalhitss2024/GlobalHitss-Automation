'''
PROYECTO:			 EMPRESAS Y NEGOCIOS
AUTOR:			     HITSS BI - JORGE MOSQUERA
FECHA:               17/07/2025
DESCRIPCION:	     SE CREA SCRIPT CON LOS ALTER REQUERIDOS PARA EL PROCESO NEGOCIOS FIJO
'''


-- TABLA tb_base_planta_comercial campo estado ampliacion de longitud

ALTER TABLE proc_genericos.tb_base_planta_comercial
ALTER COLUMN estado TYPE VARCHAR(50);
COMMIT;
