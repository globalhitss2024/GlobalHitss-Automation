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
from sqlalchemy import text

class ControlEjecucion:
    """
    Clase para manejar la auditoría y el registro de logs para procesos ETL.
    El proceso consiste en iniciar, actualizar y finalizar para cumplir con las reglas de las llaves foráneas.
    Es decir, no se puede insertar una fila en la tabla "hija" (tb_errores_cargue) si el valor
    de su llave foránea no existe ya en la tabla "padre" (tb_resumen_cargue).
    """

    ''' Para instanciar la clase ControlEjecucion, en el script principal de la ETL se deben definir estas variables'''
    def __init__(self, id_ejecucion, db_engine, log_folder, log_filename, fuente, destino):
        self.id_ejecucion = id_ejecucion
        self.db_engine = db_engine
        self.fecha_inicio = datetime.now()
        self.fuente = fuente
        self.destino = destino

        # Validar que la carpeta de logs exista 
        log_file = os.path.join(log_folder, log_filename)
        if not os.path.exists(log_folder):
            os.makedirs(log_folder)

        # Configuración del sistema de logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file, mode='w'), # Write para que el log se sobrescriba en cada ejecución
                logging.StreamHandler()
            ],
            force=True
        )
        logging.info(f"Inicio de la ejecución: {self.id_ejecucion}")
        self._iniciar_registro() # Registrar el inicio de la ejecución

    def _iniciar_registro(self):
        df_inicio = pd.DataFrame({ # DF que contiene los datos de inicio de la ejecución para tb_resumen_cargue
            'id_ejecucion': [self.id_ejecucion],
            'fecha_inicio_procesamiento': [self.fecha_inicio],
            'id_estado': [1], # Por defecto en estado exitoso
            'fuentes': [self.fuente],
            'destino': [self.destino],
            'cantidad_registros': [0],
            'duracion_segundos': [0]
        })
        try:
            df_inicio.to_sql( # Inicia poblando tb_resumen_cargue
                name='tb_resumen_cargue',
                con=self.db_engine,
                schema='control_procesamiento',
                if_exists='append',
                index=False
            )
            print("Registro de inicio creado en tb_resumen_cargue.")
        except SQLAlchemyError as e:
            logging.critical(f"FALLA CRÍTICA --> No se pudo iniciar el registro en tb_resumen_cargue: {e}")
            raise
    
    '''En el script principal se llama el siguiente método solo si todo el proceso ETL se completa sin errores.'''
    def registrar_resumen_exitoso(self, cantidad_registros): 
        fecha_fin = datetime.now()
        duracion = int((fecha_fin - self.fecha_inicio).total_seconds())

        # Se actualiza la fila que se creó al inicio (_iniciar_registro)
        sql = text("""   
            UPDATE control_procesamiento.tb_resumen_cargue
            SET fecha_fin_procesamiento = :fecha_fin,
                duracion_segundos = :duracion,
                cantidad_registros = :cantidad,
                id_estado = 1
            WHERE id_ejecucion = :id_ejecucion
        """)

        try:
            with self.db_engine.connect() as connection:
                transaccion = connection.begin()
                connection.execute(sql, {
                    "fecha_fin": fecha_fin,
                    "duracion": duracion,
                    "cantidad": cantidad_registros,
                    "id_ejecucion": self.id_ejecucion
                })
                transaccion.commit()
            logging.info(f"Ejecución Exitosa: {cantidad_registros} Registros procesados")
        except SQLAlchemyError as e:
            logging.error(f"Error al ACTUALIZAR el resumen de ejecución exitosa: {e}")

    '''En el script principal se llama desde el bloque except, solo si ocurre un error.'''
    def registrar_error(self, funcion, descripcion): 
        fecha_fin = datetime.now()
        duracion = int((fecha_fin - self.fecha_inicio).total_seconds())

        # Se actualiza la fila que se creó al inicio (_iniciar_registro) cambiando el estado a 2 
        sql_update = text("""
            UPDATE control_procesamiento.tb_resumen_cargue
            SET fecha_fin_procesamiento = :fecha_fin,
                duracion_segundos = :duracion,
                id_estado = 2
            WHERE id_ejecucion = :id_ejecucion
        """)

        try:
            with self.db_engine.connect() as connection:
                transaccion = connection.begin()
                connection.execute(sql_update, { # Se actualiza tb_resumen_cargue
                    "fecha_fin": fecha_fin,
                    "duracion": duracion,
                    "id_ejecucion": self.id_ejecucion
                })

                # Insertar el detalle del error
                df_error = pd.DataFrame({ 
                    'fecha_inicio': [self.fecha_inicio.strftime("%Y-%m-%d %H:%M:%S")],
                    'fecha_fin': [fecha_fin.strftime("%Y-%m-%d %H:%M:%S")],
                    'duracion': [duracion],
                    'fuente': [self.fuente],
                    'cantidad_registros': [0],
                    'destino': [self.destino],
                    'id_estado': [2],
                    'funcion_error': [funcion],
                    'descripcion_error': [str(descripcion)[:255]]
                })

                df_error.to_sql( # Se inserta una nueva fila en tb_errores_cargue
                    name='tb_errores_cargue',
                    con=connection,
                    schema='control_procesamiento',
                    if_exists='append',
                    index=False
                )
                transaccion.commit() # Se confirma la transacción con ambas acciones

                logging.error(f"Error en '{funcion}': {descripcion}")
        except SQLAlchemyError as e:
            # print(f"error: {e}")
            logging.error(f"Descripción: {descripcion}")