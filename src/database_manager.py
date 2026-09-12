# src/database_manager.py
"""
Gestor de conexiones a PostgreSQL con pool de conexiones
Maneja las conexiones de manera eficiente y segura
"""

import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
import logging
from typing import Dict, Any, Optional
from contextlib import contextmanager

class DatabaseManager:
    """
    Clase para manejar conexiones a PostgreSQL de manera eficiente.
    Utiliza pools de conexión para optimizar el rendimiento.
    """

    def __init__(self, db_config: Dict[str, Any], pool_size: int = 5):
        """
        Inicializa el gestor de base de datos

        Args:
            db_config: Diccionario con parámetros de conexión
            pool_size: Tamaño del pool de conexiones
        """
        self.db_config = db_config
        self.pool_size = pool_size
        self.connection_pool = None
        self.logger = logging.getLogger(__name__)
        # Último error de conexión (la GUI lo muestra al probar conexiones)
        self.last_error = None
        # Conexión reservada para ejecutar todo en una sola transacción (modo simulación)
        self._held_connection = None
        # Conexión con una sentencia en curso (para poder cancelarla desde otro hilo)
        self._active_connection = None

    def create_connection_pool(self) -> bool:
        """
        Crea un pool de conexiones a PostgreSQL

        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario
        """
        self.last_error = None
        try:
            # Crear el pool de conexiones
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=self.pool_size,
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                port=self.db_config['port'],
                connect_timeout=self.db_config.get('connect_timeout'),  # None = sin límite
                cursor_factory=RealDictCursor  # Para obtener resultados como diccionarios
            )

            # Probar la conexión
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT version();')
                    version = cursor.fetchone()

            self.logger.info(f"✅ Conexión exitosa a {self.db_config['database']} en {self.db_config['host']}")
            self.logger.info(f"📊 Versión PostgreSQL: {version['version'][:50]}...")
            return True

        except psycopg2.OperationalError as e:
            self.last_error = str(e).strip()
            self.logger.error(f"❌ Error de conexión a {self.db_config['database']}: {str(e)}")
            return False
        except Exception as e:
            self.last_error = str(e).strip()
            self.logger.error(f"❌ Error inesperado al conectar a {self.db_config['database']}: {str(e)}")
            return False

    @contextmanager
    def get_connection(self):
        """
        Context manager para obtener conexiones del pool de manera segura
        Garantiza que las conexiones se devuelvan al pool correctamente
        """
        if self._held_connection is not None:
            # Transacción única: todas las operaciones usan la misma conexión y nada se confirma aquí
            self._active_connection = self._held_connection
            try:
                yield self._held_connection
            finally:
                self._active_connection = None
            return

        connection = None
        try:
            if not self.connection_pool:
                raise Exception("Pool de conexiones no inicializado")

            # Obtener conexión del pool
            connection = self.connection_pool.getconn()
            if connection:
                self._active_connection = connection
                yield connection
            else:
                raise Exception("No se pudo obtener conexión del pool")

        except Exception as e:
            if connection:
                connection.rollback()
            raise e
        finally:
            self._active_connection = None
            if connection and self.connection_pool:
                # Devolver conexión al pool
                self.connection_pool.putconn(connection)

    def _commit(self, conn):
        """Confirma la transacción, excepto en transacción única (se decide al final)"""
        if self._held_connection is None:
            conn.commit()

    @contextmanager
    def _savepoint(self, cursor):
        """En transacción única, aísla cada operación para que un error no aborte las demás"""
        if self._held_connection is None:
            yield
            return
        cursor.execute("SAVEPOINT etl_paso")
        try:
            yield
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT etl_paso")
            raise
        cursor.execute("RELEASE SAVEPOINT etl_paso")

    def begin_single_transaction(self):
        """
        Reserva una conexión y ejecuta todo lo siguiente en una sola transacción.
        Se usa en modo simulación, que termina con end_single_transaction(commit=False).
        """
        if not self.connection_pool:
            raise Exception("Pool de conexiones no inicializado")
        self._held_connection = self.connection_pool.getconn()

    def end_single_transaction(self, commit: bool = False):
        """
        Confirma o deshace la transacción única y devuelve la conexión al pool

        Args:
            commit: True para confirmar, False para deshacer (ROLLBACK)
        """
        connection = self._held_connection
        self._held_connection = None
        if connection is None:
            return
        try:
            if commit:
                connection.commit()
            else:
                connection.rollback()
        finally:
            if self.connection_pool:
                self.connection_pool.putconn(connection)

    def cancel_running_query(self):
        """Cancela la sentencia en curso, si hay una (se puede llamar desde otro hilo)"""
        connection = self._active_connection
        if connection is not None:
            try:
                connection.cancel()
            except Exception:
                pass

    def execute_query(self, query: str, params: Optional[tuple] = None) -> list:
        """
        Ejecuta una consulta SELECT y retorna los resultados

        Args:
            query: Consulta SQL a ejecutar
            params: Parámetros para la consulta (opcional)

        Returns:
            list: Lista de diccionarios con los resultados
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    results = cursor.fetchall()

            self.logger.info(f"✅ Consulta ejecutada exitosamente. Filas obtenidas: {len(results)}")
            return results

        except Exception as e:
            self.logger.error(f"❌ Error ejecutando consulta: {str(e)}")
            raise e

    def execute_insert(self, query: str, data: Optional[list] = None) -> int:
        """
        Ejecuta un INSERT y retorna el número de filas afectadas

        Args:
            query: Consulta INSERT a ejecutar
            data: Datos para insert masivo (opcional)

        Returns:
            int: Número de filas insertadas
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    with self._savepoint(cursor):
                        if data:
                            # Insert masivo para mejor rendimiento
                            cursor.executemany(query, data)
                        else:
                            cursor.execute(query)

                        rows_affected = cursor.rowcount
                    self._commit(conn)

            self.logger.info(f"✅ Insert ejecutado exitosamente. Filas insertadas: {rows_affected}")
            return rows_affected

        except Exception as e:
            self.logger.error(f"❌ Error ejecutando insert: {str(e)}")
            raise e

    def create_temp_table_from_data(self, table_name: str, data: list, schema: str = 'public', columns_definition: dict = None) -> bool:
        """
        Crea una tabla temporal a partir de datos

        Args:
            table_name: Nombre de la tabla temporal
            data: Datos para insertar (puede ser vacío)
            schema: Esquema donde crear la tabla
            columns_definition: Definición de columnas cuando data está vacío

        Returns:
            bool: True si fue exitoso
        """
        if not data and not columns_definition:
            self.logger.warning(f"⚠️ No hay datos ni definición de columnas para crear tabla {table_name}")
            return False

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    with self._savepoint(cursor):
                        # Determinar la estructura de columnas
                        if data:
                            # Crear tabla temporal basada en la estructura del primer registro
                            first_row = data[0]
                            columns = []

                            for key, value in first_row.items():
                                # IMPORTANTE: bool debe verificarse ANTES de int
                                # porque en Python bool es subclase de int
                                if isinstance(value, bool):
                                    col_type = 'BOOLEAN'
                                elif isinstance(value, int):
                                    col_type = 'INTEGER'
                                elif isinstance(value, float):
                                    col_type = 'NUMERIC'
                                else:
                                    col_type = 'TEXT'

                                columns.append(f'"{key}" {col_type}')
                        else:
                            # Usar definición de columnas proporcionada (tabla vacía)
                            columns = [f'"{key}" {col_type}' for key, col_type in columns_definition.items()]
                            first_row = {key: None for key in columns_definition.keys()}

                        # Crear tabla temporal
                        create_query = f"""
                        CREATE TEMP TABLE {table_name} (
                            {', '.join(columns)}
                        )
                        """

                        cursor.execute(create_query)

                        # Insertar datos solo si existen
                        if data:
                            # Insertar datos en lotes para eficiencia
                            batch_size = 1000
                            for i in range(0, len(data), batch_size):
                                batch = data[i:i + batch_size]
                                values = []

                                for row in batch:
                                    row_values = [row[key] for key in first_row.keys()]
                                    values.append(row_values)

                                # Preparar query de insert
                                placeholders = ','.join(['%s'] * len(first_row.keys()))
                                insert_query = f"""
                                INSERT INTO {table_name} ({','.join(f'"{k}"' for k in first_row.keys())})
                                VALUES ({placeholders})
                                """

                                cursor.executemany(insert_query, values)

                    self._commit(conn)

            if data:
                self.logger.info(f"✅ Tabla temporal {table_name} creada con {len(data)} registros")
            else:
                self.logger.info(f"✅ Tabla temporal {table_name} creada VACÍA (sin registros)")
            return True

        except Exception as e:
            self.logger.error(f"❌ Error creando tabla temporal {table_name}: {str(e)}")
            raise e

    def drop_temp_table(self, table_name: str) -> bool:
        """
        Elimina una tabla temporal

        Args:
            table_name: Nombre de la tabla a eliminar

        Returns:
            bool: True si fue exitoso
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    with self._savepoint(cursor):
                        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    self._commit(conn)

            self.logger.info(f"🗑️ Tabla temporal {table_name} eliminada")
            return True

        except Exception as e:
            self.logger.error(f"❌ Error eliminando tabla temporal {table_name}: {str(e)}")
            return False

    def truncate_table(self, table_name: str, cascade: bool = True, skip_if_not_exists: bool = True) -> dict:
        """
        Ejecuta TRUNCATE en una tabla

        Args:
            table_name: Nombre completo de la tabla (puede incluir esquema)
            cascade: Si True, usa TRUNCATE CASCADE
            skip_if_not_exists: Si True, omite tabla si no existe en lugar de fallar

        Returns:
            dict: Diccionario con resultado {'success': bool, 'skipped': bool, 'error': str}
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Construir query TRUNCATE
                    truncate_query = f"TRUNCATE TABLE {table_name}"
                    if cascade:
                        truncate_query += " CASCADE"

                    with self._savepoint(cursor):
                        cursor.execute(truncate_query)
                    self._commit(conn)

            self.logger.info(f"🧹 Tabla {table_name} truncada exitosamente")
            return {'success': True, 'skipped': False, 'error': None}

        except Exception as e:
            error_msg = str(e)

            # Verificar si el error es porque la tabla no existe
            if 'does not exist' in error_msg.lower() or 'no existe' in error_msg.lower():
                if skip_if_not_exists:
                    self.logger.warning(f"⚠️ Tabla {table_name} no existe, omitiendo...")
                    return {'success': True, 'skipped': True, 'error': error_msg}
                else:
                    self.logger.error(f"❌ Tabla {table_name} no existe: {error_msg}")
                    return {'success': False, 'skipped': False, 'error': error_msg}
            else:
                # Otro tipo de error
                self.logger.error(f"❌ Error truncando tabla {table_name}: {error_msg}")
                return {'success': False, 'skipped': False, 'error': error_msg}

    def truncate_tables(self, table_names: list, cascade: bool = True, skip_if_not_exists: bool = True) -> dict:
        """
        Ejecuta TRUNCATE en múltiples tablas

        Args:
            table_names: Lista de nombres de tablas a truncar
            cascade: Si True, usa TRUNCATE CASCADE
            skip_if_not_exists: Si True, omite tablas que no existen

        Returns:
            dict: Diccionario con estadísticas de la operación
        """
        success_count = 0
        failed_count = 0
        skipped_count = 0
        failed_tables = []
        skipped_tables = []

        self.logger.info(f"🧹 Iniciando TRUNCATE de {len(table_names)} tabla(s)...")

        for table_name in table_names:
            result = self.truncate_table(table_name, cascade, skip_if_not_exists)

            if result['success']:
                if result['skipped']:
                    skipped_count += 1
                    skipped_tables.append(table_name)
                else:
                    success_count += 1
            else:
                failed_count += 1
                failed_tables.append({'table': table_name, 'error': result['error']})

        # Resumen
        self.logger.info(f"✅ Tablas truncadas exitosamente: {success_count}/{len(table_names)}")

        if skipped_count > 0:
            self.logger.info(f"⏭️  Tablas omitidas (no existen): {skipped_count}")
            for table in skipped_tables[:5]:  # Mostrar solo las primeras 5
                self.logger.info(f"   - {table}")
            if len(skipped_tables) > 5:
                self.logger.info(f"   ... y {len(skipped_tables) - 5} más")

        if failed_count > 0:
            self.logger.warning(f"⚠️ Tablas con errores: {failed_count}")
            for failed in failed_tables[:5]:  # Mostrar solo las primeras 5
                self.logger.warning(f"   - {failed['table']}: {failed['error'][:100]}")
            if len(failed_tables) > 5:
                self.logger.warning(f"   ... y {len(failed_tables) - 5} más")

        return {
            'total': len(table_names),
            'success': success_count,
            'failed': failed_count,
            'skipped': skipped_count,
            'failed_tables': [f['table'] for f in failed_tables],
            'skipped_tables': skipped_tables
        }

    def list_schemas(self) -> list:
        """
        Lista los esquemas disponibles en la base de datos, filtrando los del sistema.

        Returns:
            list: Lista de nombres de esquemas
        """
        query = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public', 'pg_toast', 'topology')
              AND schema_name NOT LIKE 'pg_%'
            ORDER BY schema_name
        """
        try:
            results = self.execute_query(query)
            schemas = [row['schema_name'] for row in results]
            self.logger.info(f"Esquemas encontrados: {len(schemas)}")
            return schemas
        except Exception as e:
            self.logger.error(f"Error listando esquemas: {str(e)}")
            raise e

    def close_pool(self):
        """
        Cierra el pool de conexiones
        """
        try:
            if self._held_connection is not None:
                self.end_single_transaction(commit=False)
            if self.connection_pool:
                self.connection_pool.closeall()
                self.logger.info("🔒 Pool de conexiones cerrado")
                self.connection_pool = None
        except Exception:
            pass  # Silenciar errores al cerrar el pool

    def __del__(self):
        """
        Destructor para asegurar que se cierren las conexiones
        """
        try:
            self.close_pool()
        except Exception:
            pass  # Silenciar errores en el destructor
