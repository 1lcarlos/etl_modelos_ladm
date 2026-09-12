# src/logger.py
"""
Sistema de logging personalizado para el ETL
Genera logs detallados con timestamps y información por esquema
"""

import logging
import os
from datetime import datetime
from typing import Optional

class ETLLogger:
    """
    Clase para manejar el logging del ETL de manera centralizada.
    Crea archivos de log con timestamp y maneja logs por esquema.
    """
    
    def __init__(self, log_directory: str = './logs/'):
        """
        Inicializa el sistema de logging
        
        Args:
            log_directory: Directorio donde se guardarán los logs
        """
        self.log_directory = log_directory
        self.ensure_log_directory()
        
        # Generar nombre de archivo con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_filename = f"etl_execution_{timestamp}.txt"
        self.log_filepath = os.path.join(log_directory, self.log_filename)
        
        # Configurar logging
        self.setup_logging()
        self.logger = logging.getLogger('ETL')
        
        # Estadísticas del proceso
        self.stats = {
            'start_time': datetime.now(),
            'schemas_processed': 0,
            'schemas_failed': 0,
            'total_queries': 0,
            'total_inserts': 0,
            'total_records': 0
        }
        
        self.log_process_start()
    
    def ensure_log_directory(self):
        """
        Crea el directorio de logs si no existe
        """
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)
    
    def setup_logging(self):
        """
        Configura el sistema de logging para escribir tanto en archivo como en consola
        """
        # Limpiar handlers existentes
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        # Configurar formato
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para archivo
        file_handler = logging.FileHandler(self.log_filepath, encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        
        # Handler para consola
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        
        # Configurar logger principal
        logger = logging.getLogger('ETL')
        logger.setLevel(logging.INFO)

        # Cerrar los handlers de una ejecución anterior (la GUI ejecuta varias veces en la misma
        # sesión); los que agregan otros, como el visor de la GUI, se conservan
        for handler in logger.handlers[:]:
            if getattr(handler, 'etl_logger_handler', False):
                handler.close()
                logger.removeHandler(handler)
        file_handler.etl_logger_handler = True
        console_handler.etl_logger_handler = True

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        # Evitar duplicación de logs
        logger.propagate = False
    
    def log_process_start(self):
        """
        Registra el inicio del proceso ETL
        """
        self.logger.info("=" * 80)
        self.logger.info("🚀 INICIANDO PROCESO ETL POSTGRESQL")
        self.logger.info("=" * 80)
        self.logger.info(f"📁 Archivo de log: {self.log_filename}")
        self.logger.info(f"🕐 Fecha y hora de inicio: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    def log_schema_start(self, schema: str):
        """
        Registra el inicio del procesamiento de un esquema
        
        Args:
            schema: Nombre del esquema que se va a procesar
        """
        self.logger.info("-" * 60)
        self.logger.info(f"📋 PROCESANDO ESQUEMA: {schema}")
        self.logger.info("-" * 60)
    
    def log_schema_success(self, schema: str, queries_count: int, inserts_count: int, records_count: int):
        """
        Registra el éxito en el procesamiento de un esquema
        
        Args:
            schema: Nombre del esquema procesado
            queries_count: Número de queries ejecutadas
            inserts_count: Número de inserts ejecutados
            records_count: Número de registros procesados
        """
        self.stats['schemas_processed'] += 1
        self.stats['total_queries'] += queries_count
        self.stats['total_inserts'] += inserts_count
        self.stats['total_records'] += records_count
        
        self.logger.info(f"✅ ESQUEMA {schema} PROCESADO EXITOSAMENTE")
        self.logger.info(f"   📊 Queries ejecutadas: {queries_count}")
        self.logger.info(f"   📊 Inserts ejecutados: {inserts_count}")
        self.logger.info(f"   📊 Registros procesados: {records_count}")
    
    def log_schema_error(self, schema: str, error: Exception, step: str):
        """
        Registra un error en el procesamiento de un esquema
        
        Args:
            schema: Nombre del esquema que falló
            error: Excepción ocurrida
            step: Paso del proceso donde ocurrió el error
        """
        self.stats['schemas_failed'] += 1
        
        self.logger.error(f"❌ ERROR EN ESQUEMA {schema}")
        self.logger.error(f"   🔍 Paso: {step}")
        self.logger.error(f"   💥 Error: {str(error)}")
        self.logger.error(f"   📍 Tipo de error: {type(error).__name__}")
    
    def log_query_execution(self, filename: str, schema: str, rows_affected: int):
        """
        Registra la ejecución de una query
        
        Args:
            filename: Nombre del archivo SQL ejecutado
            schema: Esquema donde se ejecutó
            rows_affected: Número de filas afectadas
        """
        self.logger.info(f"🔍 Query ejecutada: {filename} en esquema {schema} - {rows_affected} filas")
    
    def log_insert_execution(self, filename: str, rows_affected: int):
        """
        Registra la ejecución de un insert
        
        Args:
            filename: Nombre del archivo SQL ejecutado
            rows_affected: Número de filas insertadas
        """
        self.logger.info(f"📥 Insert ejecutado: {filename} - {rows_affected} filas insertadas")
    
    def log_temp_table_operation(self, operation: str, table_name: str, success: bool):
        """
        Registra operaciones en tablas temporales
        
        Args:
            operation: Tipo de operación (CREATE/DROP)
            table_name: Nombre de la tabla
            success: Si la operación fue exitosa
        """
        status = "✅" if success else "❌"
        self.logger.info(f"{status} Tabla temporal {operation}: {table_name}")
    
    def log_process_end(self):
        """
        Registra el final del proceso ETL con estadísticas
        """
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']
        
        self.logger.info("=" * 80)
        self.logger.info("🏁 PROCESO ETL FINALIZADO")
        self.logger.info("=" * 80)
        self.logger.info(f"🕐 Hora de finalización: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"⏱️ Duración total: {duration}")
        self.logger.info(f"📈 ESTADÍSTICAS FINALES:")
        self.logger.info(f"   🔢 Esquemas procesados exitosamente: {self.stats['schemas_processed']}")
        self.logger.info(f"   ❌ Esquemas con errores: {self.stats['schemas_failed']}")
        self.logger.info(f"   🔍 Total de queries ejecutadas: {self.stats['total_queries']}")
        self.logger.info(f"   📥 Total de inserts ejecutados: {self.stats['total_inserts']}")
        self.logger.info(f"   📊 Total de registros procesados: {self.stats['total_records']}")
        
        if self.stats['schemas_failed'] == 0:
            self.logger.info("🎉 PROCESO COMPLETADO SIN ERRORES")
        else:
            self.logger.warning(f"⚠️ PROCESO COMPLETADO CON {self.stats['schemas_failed']} ESQUEMAS CON ERRORES")
    
    def get_log_filepath(self) -> str:
        """
        Retorna la ruta completa del archivo de log
        
        Returns:
            str: Ruta del archivo de log
        """
        return self.log_filepath