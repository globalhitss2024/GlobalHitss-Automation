'''                                                                                                                 
PROYECTO:               ANALÍTICA DE EMPRESAS Y NEGOCIOS                                                               
FRENTE DE TRABAJO:      VISUALIZACIÓN                                                                        
AUTOR:                  HITSS - FERNANDA ZAMBRANO                                                                     
OPERACIÓN:              CREACIÓN DE TABLA                                                 
VERSIÓN:                v_1.0                                                                                           
FECHA:                  22/08/2025                                                                                      
DESCRIPCIÓN:            SCRIPT QUE PERMITE LA CREACIÓN DE LA TABLA tb_emp_neg_umc
''' 

CREATE TABLE IF NOT EXISTS public.tb_emp_neg_umc (
    "ID" SERIAL PRIMARY KEY,
    "AÑO" INTEGER,
    "MES" VARCHAR(10),
    "NUMERO_MES" INTEGER,
    "SEGMENTO" VARCHAR(25),
    "PORTAFOLIO" VARCHAR(10),
    "DIRECCION" VARCHAR(50),
    "TIPO_INDICADOR" VARCHAR(100),
    "DIVISION" VARCHAR(100),
    "SUBDIVISION" VARCHAR(100),
    "DETALLE" VARCHAR(50),
    "VALOR" FLOAT,
    "META" FLOAT
);