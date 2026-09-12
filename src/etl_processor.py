# src/etl_processor.py
"""
Procesador principal del ETL
Maneja la lógica de extracción, transformación y carga
"""

import os
import glob
import re
import threading
import time
from typing import List, Dict, Any, Set, Callable, Optional
from .database_manager import DatabaseManager
from .flujos import leer_orden, tablas_destino, tiene_control_transaccion
from .logger import ETLLogger


class ETLCancelado(Exception):
    """El usuario pidió detener el proceso"""


class ETLProcessor:
    """
    Clase principal que maneja todo el proceso ETL.
    Coordina la extracción desde base_origen y carga en base_destino.
    """

    def __init__(self, origen_config: Dict, destino_config: Dict, paths: Dict, performance_config: Dict, truncate_config: Dict = None,
                 simulate: bool = False, on_event: Optional[Callable[[str, Dict], None]] = None):
        """
        Inicializa el procesador ETL

        Args:
            origen_config: Configuración de base de datos origen
            destino_config: Configuración de base de datos destino
            paths: Rutas de archivos y directorios
            performance_config: Configuración de rendimiento
            truncate_config: Configuración de TRUNCATE (opcional)
            simulate: Si True, cada esquema se ejecuta en una sola transacción que se deshace al final
            on_event: Callback opcional (evento, datos) para seguir el avance archivo por archivo
        """
        self.paths = paths
        self.performance_config = performance_config
        self.truncate_config = truncate_config or {
            'enabled': True,
            'require_confirmation': True,
            'use_cascade': True
        }
        self.simulate = simulate
        self.on_event = on_event
        self._stop_requested = threading.Event()

        # Inicializar logger
        self.logger = ETLLogger(paths['logs'])

        # Inicializar gestores de base de datos
        self.db_origen = DatabaseManager(origen_config, performance_config['connection_pool_size'])
        self.db_destino = DatabaseManager(destino_config, performance_config['connection_pool_size'])

        # Lista para almacenar tablas temporales creadas
        self.temp_tables_created = []

        # Resumen de la última ejecución (lo usa la GUI)
        self.last_summary = None

    def connect_databases(self) -> bool:
        """
        Establece conexiones con ambas bases de datos

        Returns:
            bool: True si ambas conexiones fueron exitosas
        """
        self.logger.logger.info("🔌 Estableciendo conexiones a bases de datos...")

        # Conectar a base origen
        if not self.db_origen.create_connection_pool():
            self.logger.logger.error(f"❌ No se pudo conectar a la base de datos origen: {self.db_origen.last_error}")
            return False

        # Conectar a base destino
        if not self.db_destino.create_connection_pool():
            self.logger.logger.error(f"❌ No se pudo conectar a la base de datos destino: {self.db_destino.last_error}")
            return False

        self.logger.logger.info("✅ Conexiones establecidas exitosamente")
        return True

    def request_stop(self):
        """
        Solicita detener el proceso: cancela la sentencia en curso y no inicia archivos nuevos.
        Se puede llamar desde otro hilo.
        """
        self._stop_requested.set()
        self.db_origen.cancel_running_query()
        self.db_destino.cancel_running_query()

    def _check_stop(self):
        """Lanza ETLCancelado si se pidió detener el proceso"""
        if self._stop_requested.is_set():
            raise ETLCancelado("Proceso detenido por el usuario")

    def _emit(self, event: str, **data):
        """Notifica un evento al callback on_event, si existe"""
        if self.on_event:
            try:
                self.on_event(event, data)
            except Exception as e:
                self.logger.logger.debug(f"Error en callback de evento {event}: {str(e)}")

    def read_sql_files(self, directory: str) -> List[str]:
        """
        Lee todos los archivos SQL de un directorio

        Args:
            directory: Ruta del directorio con archivos SQL

        Returns:
            List[str]: Lista de rutas de archivos SQL
        """
        pattern = os.path.join(directory, "*.sql")
        sql_files = glob.glob(pattern)
        sql_files.sort()  # Ordenar alfabéticamente

        self.logger.logger.info(f"📁 Encontrados {len(sql_files)} archivos SQL en {directory}")
        return sql_files

    def read_insert_order(self) -> List[str]:
        """
        Lee el archivo de orden de inserts.
        Las líneas con '#' son comentarios y las que empiezan con '-' son archivos desactivados.

        Returns:
            List[str]: Lista ordenada de nombres de archivos para inserts
        """
        try:
            order, disabled = leer_orden(self.paths['order_file'])

            self.logger.logger.info(f"📋 Orden de inserts cargado: {len(order)} archivos")
            if disabled:
                self.logger.logger.info(f"⏭️  Archivos desactivados en el orden (prefijo '-'): {len(disabled)}")
            return order

        except FileNotFoundError:
            self.logger.logger.warning(f"⚠️ Archivo de orden no encontrado: {self.paths['order_file']}")
            # Fallback: usar orden alfabético
            insert_files = self.read_sql_files(self.paths['inserts'])
            return [os.path.basename(f) for f in insert_files]
        except Exception as e:
            self.logger.logger.error(f"❌ Error leyendo archivo de orden: {str(e)}")
            raise e

    def extract_target_tables_from_inserts(self, schema: str) -> Set[str]:
        """
        Extrae los nombres de las tablas destino desde los archivos INSERT

        Args:
            schema: Esquema a procesar

        Returns:
            Set[str]: Conjunto de nombres de tablas con el esquema incluido
        """
        self.logger.logger.info(f"🔍 Analizando archivos INSERT para identificar tablas destino...")

        insert_order = self.read_insert_order()
        target_tables = set()

        for filename in insert_order:
            insert_file_path = os.path.join(self.paths['inserts'], filename)

            if not os.path.exists(insert_file_path):
                continue

            try:
                # Leer archivo SQL
                with open(insert_file_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()

                # Buscar patrones INSERT INTO (ignora secuencias ili2db y nombres entre comillas)
                for table_name in tablas_destino(sql_content, schema):
                    target_tables.add(table_name)
                    self.logger.logger.debug(f"   📌 Encontrada tabla destino: {table_name} en {filename}")

            except Exception as e:
                self.logger.logger.warning(f"⚠️ Error parseando {filename}: {str(e)}")
                continue

        self.logger.logger.info(f"✅ Identificadas {len(target_tables)} tablas destino")
        return target_tables

    def replace_schema_placeholder(self, sql_content: str, schema: str) -> str:
        """
        Reemplaza el placeholder {schema} en las consultas SQL

        Args:
            sql_content: Contenido SQL con placeholders
            schema: Nombre del esquema a reemplazar

        Returns:
            str: SQL con esquema reemplazado
        """
        return sql_content.replace('{schema}', schema)

    def execute_queries_for_schema(self, schema: str) -> Dict[str, List[Dict]]:
        """
        Ejecuta todas las queries para un esquema específico

        Args:
            schema: Nombre del esquema a procesar

        Returns:
            Dict[str, List[Dict]]: Diccionario con resultados por archivo
        """
        query_files = self.read_sql_files(self.paths['queries'])
        results = {}
        total = len(query_files)

        for index, query_file in enumerate(query_files):
            self._check_stop()
            filename = os.path.basename(query_file)
            self._emit('query_start', schema=schema, archivo=filename, indice=index, total=total)
            start = time.perf_counter()

            try:
                # Leer archivo SQL
                with open(query_file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()

                # Reemplazar placeholder de esquema
                sql_with_schema = self.replace_schema_placeholder(sql_content, schema)

                # Ejecutar query
                query_results = self.db_origen.execute_query(sql_with_schema)

                # Almacenar resultados
                results[filename] = query_results

                # Log de la operación
                self.logger.log_query_execution(filename, schema, len(query_results))
                self._emit('query_done', schema=schema, archivo=filename, indice=index, total=total,
                           filas=len(query_results), estado='ok', error=None,
                           segundos=time.perf_counter() - start)

            except Exception as e:
                estado = 'cancelado' if self._stop_requested.is_set() else 'error'
                self._emit('query_done', schema=schema, archivo=filename, indice=index, total=total,
                           filas=None, estado=estado, error=str(e), segundos=time.perf_counter() - start)
                self.logger.logger.error(f"❌ Error ejecutando query {query_file} para esquema {schema}: {str(e)}")
                raise e

        return results

    def create_temp_tables(self, query_results: Dict[str, List[Dict]], schema: str) -> List[str]:
        """
        Crea tablas temporales con los resultados de las queries

        Args:
            query_results: Resultados de las queries por archivo
            schema: Nombre del esquema procesado

        Returns:
            List[str]: Lista de nombres de tablas temporales creadas
        """
        temp_tables = []

        for filename, data in query_results.items():
            self._check_stop()
            # Crear nombre de tabla temporal (sin extensión .sql)
            table_name = f"tmp_{os.path.splitext(filename)[0]}"

            try:
                if not data:
                    self.logger.logger.warning(f"⚠️ No hay datos para {filename} en esquema {schema} - Creando tabla vacía")
                    # Crear tabla vacía con una estructura genérica
                    # La estructura será TEXT para todas las columnas ya que no tenemos datos
                    columns_definition = {'placeholder_column': 'TEXT'}
                    success = self.db_destino.create_temp_table_from_data(table_name, data, columns_definition=columns_definition)
                else:
                    # Crear tabla temporal con datos
                    success = self.db_destino.create_temp_table_from_data(table_name, data)

                if success:
                    temp_tables.append(table_name)
                    self.logger.log_temp_table_operation("CREATE", table_name, True)
                else:
                    self.logger.log_temp_table_operation("CREATE", table_name, False)

            except Exception as e:
                self.logger.logger.error(f"❌ Error creando tabla temporal {table_name}: {str(e)}")
                raise e

        return temp_tables

    def execute_inserts(self, schema: str) -> Dict[str, Any]:
        """
        Ejecuta los inserts en el orden especificado

        Args:
            schema: Nombre del esquema procesado

        Returns:
            Dict[str, Any]: Diccionario con estadísticas de ejecución
        """
        insert_order = self.read_insert_order()
        total_records = 0
        inserts_executed = 0
        inserts_failed = 0
        failed_inserts = []
        total = len(insert_order)

        for index, filename in enumerate(insert_order):
            self._check_stop()
            insert_file_path = os.path.join(self.paths['inserts'], filename)

            if not os.path.exists(insert_file_path):
                self.logger.logger.warning(f"⚠️ Archivo de insert no encontrado: {filename}")
                self._emit('insert_done', schema=schema, archivo=filename, indice=index, total=total,
                           filas=None, estado='omitido', error='Archivo no encontrado', segundos=0.0)
                continue

            self._emit('insert_start', schema=schema, archivo=filename, indice=index, total=total)
            start = time.perf_counter()

            try:
                # Leer archivo SQL
                with open(insert_file_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()

                # Un COMMIT dentro del archivo confirmaría los cambios de la simulación
                if self.simulate and tiene_control_transaccion(sql_content):
                    raise Exception("El archivo contiene BEGIN/COMMIT/ROLLBACK y no se puede simular")

                # Reemplazar placeholder de esquema si es necesario
                sql_with_schema = self.replace_schema_placeholder(sql_content, schema)

                # Ejecutar insert
                rows_affected = self.db_destino.execute_insert(sql_with_schema)
                total_records += max(rows_affected, 0)  # -1 cuando la última sentencia no reporta filas
                inserts_executed += 1

                # Log de la operación
                self.logger.log_insert_execution(filename, rows_affected)
                self._emit('insert_done', schema=schema, archivo=filename, indice=index, total=total,
                           filas=rows_affected, estado='ok', error=None,
                           segundos=time.perf_counter() - start)

            except Exception as e:
                seconds = time.perf_counter() - start
                if self._stop_requested.is_set():
                    # El error viene de cancelar la sentencia en curso
                    self._emit('insert_done', schema=schema, archivo=filename, indice=index, total=total,
                               filas=None, estado='cancelado', error=str(e), segundos=seconds)
                    raise ETLCancelado("Proceso detenido por el usuario") from e

                inserts_failed += 1
                failed_inserts.append({
                    'filename': filename,
                    'error': str(e)
                })
                self.logger.logger.error(f"❌ Error ejecutando insert {filename} para esquema {schema}: {str(e)}")
                self.logger.logger.info(f"⏩ Continuando con el siguiente insert...")
                self._emit('insert_done', schema=schema, archivo=filename, indice=index, total=total,
                           filas=None, estado='error', error=str(e), segundos=seconds)

        # Resumen de ejecución
        self.logger.logger.info(f"📥 Inserts ejecutados exitosamente: {inserts_executed}/{len(insert_order)}")
        if inserts_failed > 0:
            self.logger.logger.warning(f"⚠️ Inserts fallidos: {inserts_failed}")
            for failed in failed_inserts:
                self.logger.logger.warning(f"   - {failed['filename']}: {failed['error']}")

        return {
            'total_records': total_records,
            'inserts_executed': inserts_executed,
            'inserts_failed': inserts_failed,
            'failed_inserts': failed_inserts
        }

    def validate_insert_dependencies(self, insert_files: List[str], temp_tables: List[str]) -> Dict[str, Any]:
        """
        Valida que los archivos INSERT tengan las tablas temporales necesarias

        Args:
            insert_files: Lista de archivos de insert a ejecutar
            temp_tables: Lista de tablas temporales disponibles

        Returns:
            Dict[str, Any]: Diccionario con resultados de validación
        """
        self.logger.logger.info(f"🔍 Validando dependencias entre inserts y tablas temporales...")

        warnings = []
        temp_table_names = set(temp_tables)

        for filename in insert_files:
            insert_file_path = os.path.join(self.paths['inserts'], filename)

            if not os.path.exists(insert_file_path):
                continue

            try:
                # Leer archivo SQL
                with open(insert_file_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read().lower()

                # Buscar referencias a tablas temporales (tmp_*)
                tmp_table_refs = re.findall(r'\btmp_\w+', sql_content)

                # Verificar si las tablas referenciadas existen
                missing_tables = []
                for table_ref in set(tmp_table_refs):
                    if table_ref not in temp_table_names:
                        missing_tables.append(table_ref)

                if missing_tables:
                    warning_msg = f"El archivo {filename} referencia tablas que no fueron creadas: {', '.join(missing_tables)}"
                    warnings.append(warning_msg)
                    self.logger.logger.warning(f"⚠️ {warning_msg}")

            except Exception as e:
                self.logger.logger.error(f"❌ Error validando {filename}: {str(e)}")

        if warnings:
            self.logger.logger.warning(f"⚠️ Se encontraron {len(warnings)} advertencias de dependencias")
        else:
            self.logger.logger.info(f"✅ Todas las dependencias están satisfechas")

        return {
            'warnings_count': len(warnings),
            'warnings': warnings
        }

    def truncate_target_tables_with_confirmation(self, schema: str) -> bool:
        """
        Trunca las tablas destino después de pedir confirmación al usuario

        Args:
            schema: Esquema a procesar

        Returns:
            bool: True si el truncate fue exitoso o fue omitido
        """
        try:
            # Verificar si TRUNCATE está habilitado
            if not self.truncate_config.get('enabled', True):
                self.logger.logger.info("ℹ️  TRUNCATE deshabilitado en configuración, omitiendo limpieza...")
                return True

            # Extraer tablas destino de los archivos INSERT
            target_tables = self.extract_target_tables_from_inserts(schema)

            if not target_tables:
                self.logger.logger.warning(f"⚠️ No se identificaron tablas destino para truncar")
                return True

            # Mostrar advertencia sobre backup
            use_cascade = self.truncate_config.get('use_cascade', True)
            cascade_msg = "TRUNCATE CASCADE" if use_cascade else "TRUNCATE"

            self.logger.logger.warning("=" * 80)
            self.logger.logger.warning("⚠️  ADVERTENCIA: LIMPIEZA DE TABLAS")
            self.logger.logger.warning("=" * 80)
            self.logger.logger.warning(f"📋 Se truncarán {len(target_tables)} tabla(s) en el esquema:")
            for table in sorted(target_tables):
                self.logger.logger.warning(f"   🗑️  {table}")
            self.logger.logger.warning("")
            self.logger.logger.warning("⚠️  IMPORTANTE: Se recomienda tener un backup antes de continuar")
            self.logger.logger.warning("⚠️  Esta operación eliminará TODOS los datos de estas tablas")
            if use_cascade:
                self.logger.logger.warning("⚠️  Se usará TRUNCATE CASCADE (afectará tablas dependientes)")
            self.logger.logger.warning("=" * 80)

            # Pedir confirmación si está configurado
            if self.truncate_config.get('require_confirmation', True):
                print("\n")
                try:
                    response = input("¿Desea continuar con el TRUNCATE? (escriba 'SI' para confirmar): ").strip()
                except EOFError:
                    self.logger.logger.error("❌ No se pudo obtener confirmación del usuario (modo no interactivo)")
                    self.logger.logger.info("💡 Para ejecutar sin confirmación, use: python main.py --no-confirm")
                    return False

                if response.upper() != 'SI':
                    self.logger.logger.info("❌ Operación cancelada por el usuario")
                    return False
            else:
                self.logger.logger.info("⚠️  Ejecutando TRUNCATE automáticamente (sin confirmación)")

            # Ejecutar TRUNCATE
            skip_if_not_exists = self.truncate_config.get('skip_if_not_exists', True)
            self.logger.logger.info(f"🧹 Iniciando limpieza de tablas con {cascade_msg}...")
            result = self.db_destino.truncate_tables(
                list(target_tables),
                cascade=use_cascade,
                skip_if_not_exists=skip_if_not_exists
            )
            self._emit('truncate_done', schema=schema, truncadas=result['success'],
                       omitidas=result['skipped'], fallidas=result['failed'],
                       tablas_fallidas=result['failed_tables'])

            # Resumen de resultados
            if result['success'] > 0:
                self.logger.logger.info(f"✅ Tablas truncadas: {result['success']}")

            if result['skipped'] > 0:
                self.logger.logger.info(f"⏭️  Tablas omitidas (no existen): {result['skipped']}")

            if result['failed'] > 0:
                self.logger.logger.error(f"❌ Tablas con errores: {result['failed']}")
                # Solo fallar si hay errores REALES (no tablas omitidas)
                return False

            self.logger.logger.info(f"✅ Limpieza completada: {result['success']} truncada(s), {result['skipped']} omitida(s)")
            return True

        except Exception as e:
            self.logger.logger.error(f"❌ Error durante la limpieza de tablas: {str(e)}")
            return False

    def cleanup_temp_tables(self, temp_tables: List[str]):
        """
        Elimina todas las tablas temporales creadas

        Args:
            temp_tables: Lista de nombres de tablas temporales a eliminar
        """
        self.logger.logger.info(f"🧹 Limpiando {len(temp_tables)} tablas temporales...")

        for table_name in temp_tables:
            try:
                success = self.db_destino.drop_temp_table(table_name)
                self.logger.log_temp_table_operation("DROP", table_name, success)
            except Exception as e:
                self.logger.logger.error(f"❌ Error eliminando tabla temporal {table_name}: {str(e)}")

    def _cleanup_after_error(self, temp_tables: List[str]):
        """Intenta limpiar las tablas temporales tras un error, sin propagar fallos"""
        if temp_tables:
            try:
                self.cleanup_temp_tables(temp_tables)
            except Exception:
                pass  # Silenciar errores de limpieza

    def process_schema(self, schema: str) -> bool:
        """
        Procesa un esquema completo: truncate -> queries -> temp tables -> validation -> inserts -> cleanup

        Args:
            schema: Nombre del esquema a procesar

        Returns:
            bool: True si el procesamiento fue exitoso (sin errores críticos)

        Raises:
            ETLCancelado: Si el usuario pidió detener el proceso
        """
        temp_tables = []
        insert_stats = None

        # En simulación todo el esquema corre en una sola transacción que se deshace al final
        if self.simulate:
            self.db_destino.begin_single_transaction()

        try:
            # 1. Registrar inicio del esquema
            self.logger.log_schema_start(schema)

            # 2. TRUNCATE de tablas destino (con confirmación)
            self._emit('phase', schema=schema, fase='truncate')
            self.logger.logger.info(f"🧹 Preparando limpieza de tablas para esquema {schema}...")
            if not self.truncate_target_tables_with_confirmation(schema):
                self._check_stop()
                self.logger.logger.warning("⚠️ Limpieza de tablas cancelada. Abortando procesamiento del esquema.")
                return False
            self._check_stop()

            # 3. Ejecutar queries y obtener datos
            self._emit('phase', schema=schema, fase='queries')
            self.logger.logger.info(f"🔍 Ejecutando queries para esquema {schema}...")
            query_results = self.execute_queries_for_schema(schema)
            queries_count = len(query_results)

            # 4. Crear tablas temporales
            self._emit('phase', schema=schema, fase='temporales')
            self.logger.logger.info(f"🏗️ Creando tablas temporales para esquema {schema}...")
            temp_tables = self.create_temp_tables(query_results, schema)
            self.temp_tables_created.extend(temp_tables)

            # 5. Validar dependencias
            insert_order = self.read_insert_order()
            validation_result = self.validate_insert_dependencies(insert_order, temp_tables)

            # 6. Ejecutar inserts
            self._emit('phase', schema=schema, fase='inserts')
            self.logger.logger.info(f"📥 Ejecutando inserts para esquema {schema}...")
            insert_stats = self.execute_inserts(schema)

            # 7. Limpiar tablas temporales
            self._emit('phase', schema=schema, fase='limpieza')
            self.cleanup_temp_tables(temp_tables)

            # 8. Registrar resultado
            total_records = insert_stats['total_records']
            inserts_count = insert_stats['inserts_executed']
            inserts_failed = insert_stats['inserts_failed']

            if inserts_failed == 0:
                self.logger.log_schema_success(schema, queries_count, inserts_count, total_records)
                return True
            else:
                # Hubo algunos fallos pero el proceso continuó
                self.logger.logger.warning(f"⚠️ Esquema {schema} procesado con {inserts_failed} insert(s) fallido(s)")
                self.logger.logger.info(f"✅ {inserts_count} insert(s) ejecutado(s) exitosamente con {total_records} registro(s)")
                # Consideramos exitoso si al menos algunos inserts funcionaron
                return inserts_count > 0

        except ETLCancelado:
            self.logger.logger.warning(f"⏹️ Procesamiento del esquema {schema} detenido por el usuario")
            self._cleanup_after_error(temp_tables)
            raise

        except Exception as e:
            if self._stop_requested.is_set():
                # El error viene de cancelar la sentencia en curso
                self.logger.logger.warning(f"⏹️ Procesamiento del esquema {schema} detenido por el usuario")
                self._cleanup_after_error(temp_tables)
                raise ETLCancelado("Proceso detenido por el usuario") from e

            # Determinar en qué paso ocurrió el error
            if not temp_tables:
                step = "Ejecución de queries o creación de tablas temporales"
            elif insert_stats is None:
                step = "Preparación de inserts"
            else:
                step = "Ejecución de inserts"

            # Registrar error
            self.logger.log_schema_error(schema, e, step)

            # Intentar limpiar tablas temporales creadas
            self._cleanup_after_error(temp_tables)

            return False

        finally:
            if self.simulate:
                self.db_destino.end_single_transaction(commit=False)
                self.logger.logger.info(f"↩️  Simulación: se deshicieron todos los cambios del esquema {schema}")

    def process_all_schemas(self, schemas: List[str],
                            on_schema_error: Optional[Callable[[str, str], bool]] = None,
                            progress_callback: Optional[Callable[[str, int, int], None]] = None) -> Dict[str, Any]:
        """
        Procesa todos los esquemas en la lista

        Args:
            schemas: Lista de nombres de esquemas a procesar
            on_schema_error: Callback al fallar un esquema. Recibe (schema, error_msg),
                           retorna True para continuar, False para detener.
                           Si es None, continua automaticamente.
            progress_callback: Callback de progreso. Recibe (schema_actual, indice, total).

        Returns:
            Dict[str, Any]: Resumen del procesamiento
        """
        self.logger.logger.info(f"📋 Iniciando procesamiento de {len(schemas)} esquemas: {schemas}")

        successful_schemas = []
        failed_schemas = []
        cancelled_schema = None
        stopped = False

        for i, schema in enumerate(schemas):
            if stopped or self._stop_requested.is_set():
                stopped = True
                break

            self.logger.logger.info(f"🎯 Procesando esquema {i+1}/{len(schemas)}: {schema}")

            if progress_callback:
                progress_callback(schema, i, len(schemas))
            self._emit('schema_start', schema=schema, indice=i, total=len(schemas))

            try:
                success = self.process_schema(schema)
                self._emit('schema_end', schema=schema, estado='ok' if success else 'error')

                if success:
                    successful_schemas.append(schema)
                    self.logger.logger.info(f"✅ Esquema {schema} procesado exitosamente")
                else:
                    failed_schemas.append(schema)
                    error_msg = f"Fallo el procesamiento del esquema {schema}"
                    self.logger.logger.error(f"❌ {error_msg}")

                    if on_schema_error and len(schemas) > 1:
                        should_continue = on_schema_error(schema, error_msg)
                        if not should_continue:
                            self.logger.logger.info("⏹️ Detenido por el usuario")
                            stopped = True

            except ETLCancelado:
                cancelled_schema = schema
                stopped = True
                self._emit('schema_end', schema=schema, estado='cancelado')
                self.logger.logger.info("⏹️ Detenido por el usuario")

            except Exception as e:
                failed_schemas.append(schema)
                error_msg = str(e)
                self._emit('schema_end', schema=schema, estado='error')
                self.logger.logger.error(f"❌ Error critico procesando esquema {schema}: {error_msg}")

                if on_schema_error and len(schemas) > 1:
                    should_continue = on_schema_error(schema, error_msg)
                    if not should_continue:
                        self.logger.logger.info("⏹️ Detenido por el usuario")
                        stopped = True

        # Resumen final
        summary = {
            'total_schemas': len(schemas),
            'successful_schemas': successful_schemas,
            'failed_schemas': failed_schemas,
            'success_count': len(successful_schemas),
            'failed_count': len(failed_schemas),
            'stopped_by_user': stopped,
            'cancelled_schema': cancelled_schema,
            'simulated': self.simulate
        }

        return summary

    def run_etl(self, schemas: List[str],
                on_schema_error: Optional[Callable[[str, str], bool]] = None,
                progress_callback: Optional[Callable[[str, int, int], None]] = None) -> bool:
        """
        Ejecuta el proceso ETL completo

        Args:
            schemas: Lista de esquemas a procesar
            on_schema_error: Callback al fallar un esquema (ver process_all_schemas)
            progress_callback: Callback de progreso (ver process_all_schemas)

        Returns:
            bool: True si el proceso general fue exitoso
        """
        try:
            # 1. Conectar a bases de datos
            if not self.connect_databases():
                self.logger.logger.error("❌ No se pudieron establecer las conexiones")
                return False

            # 2. Validar que existan los directorios necesarios
            if not self.validate_directories():
                return False

            # 3. Procesar todos los esquemas
            summary = self.process_all_schemas(schemas, on_schema_error, progress_callback)
            self.last_summary = summary

            # 4. Generar reporte final
            self.generate_final_report(summary)

            return summary['failed_count'] == 0 and not summary['stopped_by_user']

        except Exception as e:
            self.logger.logger.error(f"❌ Error crítico en el proceso ETL: {str(e)}")
            return False
        finally:
            # Asegurar limpieza de recursos
            self.cleanup_resources()

    def validate_directories(self) -> bool:
        """
        Valida que existan los directorios necesarios y contengan archivos

        Returns:
            bool: True si la validación es exitosa
        """
        # Validar directorio de queries
        if not os.path.exists(self.paths['queries']):
            self.logger.logger.error(f"❌ Directorio de queries no existe: {self.paths['queries']}")
            return False

        query_files = self.read_sql_files(self.paths['queries'])
        if not query_files:
            self.logger.logger.error(f"❌ No se encontraron archivos SQL en: {self.paths['queries']}")
            return False

        # Validar directorio de inserts
        if not os.path.exists(self.paths['inserts']):
            self.logger.logger.error(f"❌ Directorio de inserts no existe: {self.paths['inserts']}")
            return False

        insert_files = self.read_sql_files(self.paths['inserts'])
        if not insert_files:
            self.logger.logger.error(f"❌ No se encontraron archivos SQL en: {self.paths['inserts']}")
            return False

        self.logger.logger.info("✅ Validación de directorios completada")
        return True

    def generate_final_report(self, summary: Dict[str, Any]):
        """
        Genera el reporte final del proceso

        Args:
            summary: Resumen del procesamiento
        """
        self.logger.logger.info("=" * 80)
        self.logger.logger.info("📊 REPORTE FINAL DEL PROCESO ETL")
        self.logger.logger.info("=" * 80)
        self.logger.logger.info(f"   📈 Esquemas procesados exitosamente: {summary['success_count']}/{summary['total_schemas']}")

        if summary['successful_schemas']:
            self.logger.logger.info(f"   ✅ Esquemas exitosos: {', '.join(summary['successful_schemas'])}")

        if summary['failed_schemas']:
            self.logger.logger.info(f"   ❌ Esquemas con errores: {', '.join(summary['failed_schemas'])}")

        if summary.get('stopped_by_user'):
            where = f" durante el esquema {summary['cancelled_schema']}" if summary.get('cancelled_schema') else ""
            self.logger.logger.warning(f"   ⏹️ Proceso detenido por el usuario{where}")

        # Calcular porcentaje de éxito
        if summary['total_schemas'] > 0:
            success_rate = (summary['success_count'] / summary['total_schemas']) * 100
            self.logger.logger.info(f"   📊 Tasa de éxito: {success_rate:.1f}%")

        # Información adicional sobre el proceso
        if summary.get('partial_success_count', 0) > 0:
            self.logger.logger.warning(f"   ⚠️ Esquemas con éxito parcial: {summary['partial_success_count']}")
            self.logger.logger.info(f"      (Algunos inserts fallaron pero el proceso continuó)")

        if self.simulate:
            self.logger.logger.info("   🧪 MODO SIMULACIÓN: no se guardó ningún cambio en la base destino")

        self.logger.logger.info("=" * 80)

        # Finalizar log
        self.logger.log_process_end()

    def cleanup_resources(self):
        """
        Limpia todos los recursos utilizados
        """
        try:
            # Limpiar cualquier tabla temporal que pueda haber quedado
            if self.temp_tables_created:
                self.logger.logger.info("🧹 Limpieza final de tablas temporales...")
                self.cleanup_temp_tables(self.temp_tables_created)

            # Cerrar pools de conexión
            self.db_origen.close_pool()
            self.db_destino.close_pool()

            self.logger.logger.info("🔒 Recursos liberados correctamente")

        except Exception as e:
            self.logger.logger.error(f"⚠️ Error durante limpieza de recursos: {str(e)}")

    def get_log_path(self) -> str:
        """
        Retorna la ruta del archivo de log generado

        Returns:
            str: Ruta completa del archivo de log
        """
        return self.logger.get_log_filepath()
