'''                                                                                                                 
PROYECTO:               ANALÍTICA DE EMPRESAS Y NEGOCIOS                                                               
FRENTE DE TRABAJO:      VISUALIZACIÓN                                                                        
AUTOR:                  HITSS - FERNANDA ZAMBRANO                                                                     
OPERACIÓN:              CREACIÓN DE TABLA                                                 
VERSIÓN:                v_1.0                                                                                           
FECHA:                  22/08/2025                                                                                      
DESCRIPCIÓN:            SCRIPT QUE PERMITE LA CREACIÓN DE LA TABLA tb_emp_metas_fijo_movil
''' 

CREATE TABLE IF NOT EXISTS public.tb_emp_metas_fijo_movil (
    "ID" SERIAL PRIMARY KEY,
    "CEDULA" VARCHAR(25),
    "JEFATURA" VARCHAR(50),
    "GERENCIA" VARCHAR(50),
    "DIRECCION" VARCHAR(50),
    "PLANTA_COMERCIAL" VARCHAR(100),
    "CARGO_ACTUAL" VARCHAR(50),
    "MES" VARCHAR(10),
    "FECHA" DATE,
    "RETO_ESTRATEGICO" FLOAT,
    "CONVENCIONAL" FLOAT,
    "MULTINACIONALES" FLOAT,
    "META_FIJO" FLOAT,
    "NETO_FIJO_TRIMESTRAL" FLOAT,
    "LINEAS_ALTAS" FLOAT,
    "ALTAS_MOVIL" FLOAT,
    "LINEAS_BAJAS" FLOAT,
    "BAJAS_MOVIL" FLOAT,
    "CAMBIO_PLAN" FLOAT,
    "NETO_MOVIL" FLOAT,
    "RETO_MOVIL" FLOAT
);