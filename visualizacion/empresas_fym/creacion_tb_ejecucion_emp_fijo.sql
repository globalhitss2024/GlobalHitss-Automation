'''                                                                                                                 
PROYECTO:               ANALÍTICA DE EMPRESAS Y NEGOCIOS                                                               
FRENTE DE TRABAJO:      VISUALIZACIÓN                                                                        
AUTOR:                  HITSS - FERNANDA ZAMBRANO                                                                     
OPERACIÓN:              CREACIÓN DE TABLA                                                 
VERSIÓN:                v_1.0                                                                                           
FECHA:                  22/08/2025                                                                                      
DESCRIPCIÓN:            SCRIPT QUE PERMITE LA CREACIÓN DE LA TABLA tb_emp_ejecucion_emp_fijo
''' 

CREATE TABLE IF NOT EXISTS sch_emp_fijo.tb_ejecucion_emp_fijo (
    "IDENTIFICADOR" SERIAL PRIMARY KEY,
    "TIPO" VARCHAR(25),
    "CONTAR_VENTAS" VARCHAR(10),
    "ANO" INTEGER,
    "MES" DATE,
    "FECHA" DATE,
    "ID" VARCHAR(20),
    "OT" VARCHAR(20),
    "NIT" VARCHAR(20),
    "RAZON_SOCIAL" VARCHAR(200),
    "PRODUCTO" VARCHAR(25),
    "ITO" VARCHAR(25),
    "DIRECCION" VARCHAR(25),
    "CC_GERENTE" VARCHAR(25),
    "GERENCIA" VARCHAR(100),
    "CC_CONSULTOR" VARCHAR(25),
    "CONSULTOR" VARCHAR(100),
    "CC_COORDINADOR" VARCHAR(25),
    "COORDINADOR" VARCHAR(100),
    "COORDINADOR_IT" VARCHAR(100),
    "CONSULTOR_IT" VARCHAR(100),
    "CC_COORDINADOR_IT" VARCHAR(25),
    "CC_CONSULTOR_IT" VARCHAR(25),
    "TOTAL_VENTAS" FLOAT,
    "COMISIONES" FLOAT,
    "ENLACE" VARCHAR(25),
    "ACUERDOS_INDICADOR" VARCHAR(50),
    "ESTADO_OT" VARCHAR(25),
    "RED" VARCHAR(25),
    "CLASE_VENTA" VARCHAR(25),
    "PROYECTO" VARCHAR(25),
    "PROYECTO_ESPECIAL" VARCHAR(25),
    "DURACION_CONTRATO" FLOAT,
    "SERVICIO" VARCHAR(200),
    "MULTINACIONALES" VARCHAR(25)
);
