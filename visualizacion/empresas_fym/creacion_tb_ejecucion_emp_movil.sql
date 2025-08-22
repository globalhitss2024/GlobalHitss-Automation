'''                                                                                                                 
PROYECTO:               ANALÍTICA DE EMPRESAS Y NEGOCIOS                                                               
FRENTE DE TRABAJO:      VISUALIZACIÓN                                                                        
AUTOR:                  HITSS - FERNANDA ZAMBRANO                                                                     
OPERACIÓN:              CREACIÓN DE TABLA                                                 
VERSIÓN:                v_1.0                                                                                           
FECHA:                  22/08/2025                                                                                      
DESCRIPCIÓN:            SCRIPT QUE PERMITE LA CREACIÓN DE LA TABLA tb_emp_ejecucion_emp_movil
''' 

CREATE TABLE IF NOT EXISTS sch_emp_movil.tb_ejecucion_emp_movil (
    "ID" SERIAL PRIMARY KEY,
    "TIPO" VARCHAR(25),
    "MES" DATE,
    "ANO" INTEGER,
    "ZONA" VARCHAR(25),
    "RAZON" VARCHAR(100),
    "DONANTE_RECEPTOR" VARCHAR(100),
    "TRANSACCIONAL" VARCHAR(25),
    "RAZON_SOCIAL" VARCHAR(200),
    "NIT" VARCHAR(20),
    "RANGO_CFM" VARCHAR(100),
    "CODIGO" VARCHAR(25),
    "NOMBRE_CONSULTOR" VARCHAR(100),
    "PLAN" VARCHAR(200),
    "IDENTIF_MTR" INTEGER,
    "CUENTA_GERENCIA" VARCHAR(100),
    "COORDINADOR" VARCHAR(100),
    "TIPO_BASE" VARCHAR(100),
    "CFM" FLOAT,
    "LINEAS" INTEGER,
    "GERENCIA_BASE" VARCHAR(100),
    "CUENTA_DIRECCION" VARCHAR(100),
    "DIRECCION_BASE" VARCHAR(100),
    "CEDULA_CONS" VARCHAR(25),
    "CEDULA_COORD" VARCHAR(25),
    "CEDULA_GERENTE" VARCHAR(25),
    "DETALLE" VARCHAR(100),
    "SPLIT_BILLING" VARCHAR(25),
    "DISTRIBUIDOR" VARCHAR(50),
    "FAMILIA_PRODUCTOS" VARCHAR(50),
    "NRO_LINEAS" INTEGER
);
