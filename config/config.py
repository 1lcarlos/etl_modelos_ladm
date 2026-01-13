# config/config.py
"""
Configuracion centralizada para el ETL PostgreSQL
Version Windows - Usa variables de entorno y rutas relativas
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno desde archivo .env
# Busca el .env en el directorio raiz del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / '.env')

def get_env(key: str, default: str = None, cast_type: type = str):
    """
    Obtiene una variable de entorno con tipo especifico

    Args:
        key: Nombre de la variable de entorno
        default: Valor por defecto si no existe
        cast_type: Tipo al cual convertir (str, int, bool)

    Returns:
        Valor de la variable de entorno convertido al tipo especificado
    """
    value = os.getenv(key, default)

    if value is None:
        return default

    if cast_type == bool:
        return value.lower() in ('true', '1', 'yes', 'si')

    return cast_type(value)

# Configuracion de base de datos origen
DB_ORIGEN = {
    'host': get_env('DB_ORIGEN_HOST', 'localhost'),
    'database': get_env('DB_ORIGEN_DATABASE', 'interno_acc'),
    'user': get_env('DB_ORIGEN_USER', 'postgres'),
    'password': get_env('DB_ORIGEN_PASSWORD', ''),
    'port': get_env('DB_ORIGEN_PORT', '5433', int)
}

# Configuracion de base de datos destino
DB_DESTINO = {
    'host': get_env('DB_DESTINO_HOST', 'localhost'),
    'database': get_env('DB_DESTINO_DATABASE', 'interno_v3_acc'),
    'user': get_env('DB_DESTINO_USER', 'postgres'),
    'password': get_env('DB_DESTINO_PASSWORD', ''),
    'port': get_env('DB_DESTINO_PORT', '5433', int)
}

# Lista de esquemas a procesar (desde variable de entorno o default)
_schemas_str = get_env('SCHEMAS', 'cun25489')
SCHEMAS = [s.strip() for s in _schemas_str.split(',') if s.strip()]

# Configuracion de rutas (RELATIVAS al directorio base del proyecto)
PATHS = {
    'queries': str(BASE_DIR / 'sql' / 'queries'),
    'inserts': str(BASE_DIR / 'sql' / 'inserts'),
    'logs': str(BASE_DIR / 'logs'),
    'order_file': str(BASE_DIR / 'sql' / 'insert_order.txt')
}

# Configuracion de rendimiento
PERFORMANCE = {
    'batch_size': get_env('BATCH_SIZE', '1000', int),
    'connection_pool_size': get_env('CONNECTION_POOL_SIZE', '5', int),
    'max_retries': get_env('MAX_RETRIES', '3', int),
    'timeout': get_env('TIMEOUT', '300', int)
}

# Configuracion de TRUNCATE
TRUNCATE_CONFIG = {
    'enabled': get_env('TRUNCATE_ENABLED', 'true', bool),
    'require_confirmation': get_env('TRUNCATE_REQUIRE_CONFIRMATION', 'true', bool),
    'use_cascade': get_env('TRUNCATE_USE_CASCADE', 'true', bool),
    'skip_if_not_exists': get_env('TRUNCATE_SKIP_IF_NOT_EXISTS', 'true', bool)
}

# Configuracion de logging
LOGGING = {
    'level': get_env('LOG_LEVEL', 'INFO'),
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S'
}

# Funcion para verificar configuracion
def validate_config():
    """
    Valida que la configuracion minima este presente

    Raises:
        ValueError: Si falta configuracion critica
    """
    errors = []

    if not DB_ORIGEN['password']:
        errors.append("DB_ORIGEN_PASSWORD no esta configurado")

    if not DB_DESTINO['password']:
        errors.append("DB_DESTINO_PASSWORD no esta configurado")

    if not SCHEMAS:
        errors.append("SCHEMAS no esta configurado o esta vacio")

    if errors:
        raise ValueError("Errores de configuracion:\n" + "\n".join(f"  - {e}" for e in errors))

    return True

# Funcion para mostrar configuracion (sin passwords)
def print_config():
    """
    Imprime la configuracion actual (ocultando passwords)
    """
    print("=" * 60)
    print("CONFIGURACION ETL")
    print("=" * 60)
    print(f"Base origen: {DB_ORIGEN['host']}:{DB_ORIGEN['port']}/{DB_ORIGEN['database']}")
    print(f"Base destino: {DB_DESTINO['host']}:{DB_DESTINO['port']}/{DB_DESTINO['database']}")
    print(f"Esquemas: {SCHEMAS}")
    print(f"Pool conexiones: {PERFORMANCE['connection_pool_size']}")
    print(f"Batch size: {PERFORMANCE['batch_size']}")
    print(f"TRUNCATE habilitado: {TRUNCATE_CONFIG['enabled']}")
    print(f"Requiere confirmacion: {TRUNCATE_CONFIG['require_confirmation']}")
    print("=" * 60)
