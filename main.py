# main.py
"""
Archivo principal del ETL PostgreSQL
Punto de entrada para ejecutar el proceso completo

Uso:
    python main.py                    # Modo interactivo (pide confirmacion)
    python main.py --no-confirm       # Modo automatico (sin confirmacion)
    python main.py --help             # Muestra ayuda
    python main.py --version          # Muestra version
    python main.py --config           # Muestra configuracion actual
"""

import sys
import os
import argparse
from datetime import datetime

# Configurar codificacion UTF-8 para Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Anadir el directorio actual al path para imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Imports locales
from config.config import (
    DB_ORIGEN, DB_DESTINO, SCHEMAS, PATHS, PERFORMANCE, TRUNCATE_CONFIG,
    validate_config, print_config
)
from src.etl_processor import ETLProcessor

__version__ = '2.0.0'
__author__ = 'ETL Team'

def parse_arguments():
    """
    Parsea los argumentos de linea de comandos

    Returns:
        argparse.Namespace: Argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description='ETL PostgreSQL - Migracion de datos catastrales LADM-COL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Ejemplos de uso:
  python main.py                    Ejecuta ETL en modo interactivo
  python main.py --no-confirm       Ejecuta ETL sin pedir confirmacion (para tareas programadas)
  python main.py --config           Muestra la configuracion actual
  python main.py --dry-run          Simula ejecucion sin hacer cambios

Para tareas programadas (Task Scheduler), use:
  python main.py --no-confirm >> logs\\cron.log 2>&1
        '''
    )

    parser.add_argument(
        '--no-confirm',
        action='store_true',
        help='Ejecuta sin pedir confirmacion para TRUNCATE (modo automatico)'
    )

    parser.add_argument(
        '--config',
        action='store_true',
        help='Muestra la configuracion actual y termina'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simula la ejecucion sin hacer cambios reales'
    )

    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {__version__}'
    )

    parser.add_argument(
        '--schemas',
        type=str,
        help='Esquemas a procesar (separados por coma), sobreescribe configuracion'
    )

    return parser.parse_args()


def main():
    """
    Funcion principal que ejecuta el ETL completo
    """
    # Parsear argumentos
    args = parse_arguments()

    # Si solo quiere ver la configuracion
    if args.config:
        print_config()
        return True

    print("=" * 80)
    print("  ETL POSTGRESQL - MIGRACION LADM-COL")
    print("=" * 80)
    print(f"  Version: {__version__}")
    print(f"  Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Modo: {'AUTOMATICO (sin confirmacion)' if args.no_confirm else 'INTERACTIVO'}")
    print("=" * 80)

    # Validar configuracion
    try:
        validate_config()
    except ValueError as e:
        print(f"\n Error de configuracion:\n{e}")
        print("\nPor favor, configure el archivo .env correctamente.")
        print("Puede copiar .env.example como punto de partida:")
        print("  copy .env.example .env")
        return False

    # Determinar esquemas a procesar
    schemas_to_process = SCHEMAS
    if args.schemas:
        schemas_to_process = [s.strip() for s in args.schemas.split(',') if s.strip()]
        print(f"  Esquemas (override): {schemas_to_process}")

    # Preparar configuracion de TRUNCATE
    truncate_config = TRUNCATE_CONFIG.copy()
    if args.no_confirm:
        truncate_config['require_confirmation'] = False
        print("  TRUNCATE: Ejecutara automaticamente sin confirmacion")

    if args.dry_run:
        truncate_config['enabled'] = False
        print("  MODO DRY-RUN: No se ejecutaran cambios reales")

    print("=" * 80)

    # Crear instancia del procesador ETL
    etl = ETLProcessor(
        origen_config=DB_ORIGEN,
        destino_config=DB_DESTINO,
        paths=PATHS,
        performance_config=PERFORMANCE,
        truncate_config=truncate_config
    )

    try:
        # Ejecutar el proceso ETL
        success = etl.run_etl(schemas_to_process)

        # Mostrar resultado final
        if success:
            print("\n" + "=" * 80)
            print("  PROCESO ETL COMPLETADO EXITOSAMENTE")
            print("=" * 80)
            print(f"  Log generado en: {etl.get_log_path()}")
        else:
            print("\n" + "=" * 80)
            print("  PROCESO ETL COMPLETADO CON ERRORES")
            print("=" * 80)
            print(f"  Revisar log para detalles: {etl.get_log_path()}")

        return success

    except KeyboardInterrupt:
        print("\n\nProceso interrumpido por el usuario (Ctrl+C)")
        etl.cleanup_resources()
        return False

    except Exception as e:
        print(f"\nError critico no manejado: {str(e)}")
        etl.cleanup_resources()
        return False


if __name__ == "__main__":
    """
    Punto de entrada del script
    """
    try:
        success = main()
        # Codigo de salida basado en el resultado
        sys.exit(0 if success else 1)

    except Exception as e:
        print(f"Error fatal: {str(e)}")
        sys.exit(1)
