# test_truncate.py
"""
Script de prueba para verificar la extracción de tablas destino
"""

import sys
import os

# Configurar codificación UTF-8 para Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Añadir el directorio src al path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from config.config import DB_ORIGEN, DB_DESTINO, SCHEMAS, PATHS, PERFORMANCE
from src.etl_processor import ETLProcessor

def test_extract_tables():
    """
    Prueba la extracción de tablas destino
    """
    print("🧪 Iniciando prueba de extracción de tablas destino...")
    print("=" * 80)

    # Crear instancia del procesador ETL
    etl = ETLProcessor(
        origen_config=DB_ORIGEN,
        destino_config=DB_DESTINO,
        paths=PATHS,
        performance_config=PERFORMANCE
    )

    # Probar con el primer esquema
    schema = SCHEMAS[0]
    print(f"\n📋 Extrayendo tablas para el esquema: {schema}")
    print("-" * 80)

    try:
        target_tables = etl.extract_target_tables_from_inserts(schema)

        print(f"\n✅ Se identificaron {len(target_tables)} tablas destino:")
        print("-" * 80)
        for table in sorted(target_tables):
            print(f"   🗑️  {table}")
        print("-" * 80)

        return True

    except Exception as e:
        print(f"\n❌ Error durante la extracción: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_extract_tables()
    sys.exit(0 if success else 1)
