'''                                                                                                                 
PROYECTO:               ANALÍTICA DE EMPRESAS Y NEGOCIOS                                                               
FRENTE DE TRABAJO:      DESARROLLO TRANSVERSAL                                                                        
AUTOR:                  HITSS - FERNANDA ZAMBRANO                                                                     
OPERACIÓN:              UTIL QUE PERMITE AUDITORIA Y MANEJO DE LOGS                                                  
VERSIÓN:                v_1.0                                                                                           
FECHA:                  08/08/2025                                                                                      
DESCRIPCIÓN:            SCRIPT QUE PERMITE CARGAR LAS TABLAS DE AUDITORIA tb_resumen_cargue Y tb_errores_cargue         
''' 

import logging
import os
from datetime import datetime
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

class ControlEjecucion:
    """
    Clase para manejar la auditoría y el logging de un proceso ETL.
    Interactúa con las tablas de resumen y errores, y escribe en un archivo log.
    """
    def __init__(self, id_ejecucion, db_engine, log_folder, log_filename):
        self.id_ejecucion = id_ejecucion
        self.db_engine = db_engine
        self.fecha_inicio = datetime.now()
        
        # Configuración del logging
        log_file = os.path.join(log_folder, log_filename)
        if not os.path.exists(log_folder):
            os.makedirs(log_folder)
        
        # Forzamos la reconfiguración del logger para cada instancia
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file, mode='a'),
                logging.StreamHandler()
            ],
            force=True
        )
        logging.info(f"Inicio de la ejecución: {self.id_ejecucion}")

    def registrar_resumen_exitoso(self, fuente, destino, cantidad_registros):
        """
        Registra una ejecución exitosa en la tabla de resumen y en el log.
        """
        fecha_fin = datetime.now()
        duracion = (fecha_fin - self.fecha_inicio).total_seconds()
        
        df_resumen = pd.DataFrame({
            'id_ejecucion': [self.id_ejecucion],
            'fecha_inicio_procesamiento': [self.fecha_inicio],
            'fecha_fin_procesamiento': [fecha_fin],
            'duracion_segundos': [duracion],
            'fuentes': [fuente],
            'cantidad_registros': [cantidad_registros],
            'destino': [destino],
            'id_estado': [1], # 1 = Exitoso
        })
        
        try:
            df_resumen.to_sql(
                name='tb_resumen_cargue',
                con=self.db_engine,
                schema='control_procesamiento',
                if_exists='append',
                index=False
            )
            logging.info(f"Resumen de ejecución exitosa registrado para {self.id_ejecucion}.")
            logging.info(f"Fin de la ejecución: {self.id_ejecucion} con estado: Exitoso")
        except SQLAlchemyError as e:
            log_msg = f"Error al registrar el resumen de ejecución exitosa: {e}"
            logging.error(log_msg)
            # Si falla el registro del resumen, se registra como un error del proceso
            self.registrar_error(
                funcion='ControlEjecucion.registrar_resumen_exitoso',
                descripcion=log_msg,
                fuente=fuente,
                destino=destino
            )

    def registrar_error(self, funcion, descripcion, fuente, destino):
        """
        Registra una ejecución fallida en la tabla de errores y en el log.
        """
        fecha_fin = datetime.now()
        duracion = (fecha_fin - self.fecha_inicio).total_seconds()

        df_error = pd.DataFrame({
            'id_ejecucion': [self.id_ejecucion],
            'fecha_inicio': [self.fecha_inicio],
            'fecha_fin': [fecha_fin],
            'duracion': [duracion],
            'fuente': [fuente],
            'cantidad_registros': [0],
            'destino': [destino],
            'id_estado': [2], # 2 = Fallido
            'funcion_error': [funcion],
            'descripcion_error': [str(descripcion)[:255]] # Truncar para no exceder el límite de la DB
        })

        try:
            df_error.to_sql(
                name='tb_errores_cargue',
                con=self.db_engine,
                schema='control_procesamiento',
                if_exists='append',
                index=False
            )
            logging.error(f"Error en la función '{funcion}': {descripcion}")
            logging.info(f"Fin de la ejecución: {self.id_ejecucion} con estado: Fallido")
        except SQLAlchemyError as e:
            # Si falla hasta el registro del error, solo queda el log de archivo
            logging.error(f"FALLO CRÍTICO: No se pudo registrar el error en la base de datos: {e}")
            logging.error(f"Error original -> Función: {funcion}, Descripción: {descripcion}")
