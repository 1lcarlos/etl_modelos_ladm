# verificar_tablas.py
"""
Script para verificar qué tablas existen realmente en la base de datos
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

from config.config import DB_DESTINO, SCHEMAS
from src.database_manager import DatabaseManager

def verificar_tablas_en_bd():
    """
    Verifica qué tablas existen en la base de datos destino
    """
    print("🔍 Verificando tablas en la base de datos destino...")
    print("=" * 80)

    # Conectar a la base de datos destino
    db = DatabaseManager(DB_DESTINO, 5)

    if not db.create_connection_pool():
        print("❌ No se pudo conectar a la base de datos")
        return

    # Lista de tablas que el sistema identificó
    tablas_identificadas = [
        'cc_barrio', 'cc_centropoblado', 'cc_corregimiento', 'cc_limitemunicipio',
        'cc_localidadcomuna', 'cc_manzana', 'cc_nomenclaturavial', 'cc_perimetrourbano',
        'cc_sectorrural', 'cc_sectorurbano', 'cc_vereda', 'col_masccl', 'col_menosccl',
        'col_puntoccl', 'col_uebaunit', 'extdireccion', 'gc_agrupacioninteresados',
        'gc_calificacionconvencional', 'gc_calificacionnoconvencional',
        'gc_estructuraalertapredio', 'gc_estructuraavaluo', 'gc_interesado',
        'gc_lindero', 'gc_predio', 'gc_puntocontrol', 'gc_puntolindero',
        'gc_terreno', 'gc_unidadconstruccion'
    ]

    schema = SCHEMAS[0]  # Usar el primer esquema

    print(f"\n📋 Verificando tablas en el esquema: {schema}")
    print("-" * 80)

    tablas_existentes = []
    tablas_no_existentes = []

    for tabla in tablas_identificadas:
        tabla_completa = f"{schema}.{tabla}"

        # Query para verificar si la tabla existe
        query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_name = '{tabla}'
        ) as existe;
        """

        try:
            resultado = db.execute_query(query)
            existe = resultado[0]['existe'] if resultado else False

            if existe:
                # Contar registros
                count_query = f"SELECT COUNT(*) as total FROM {tabla_completa};"
                count_result = db.execute_query(count_query)
                total = count_result[0]['total'] if count_result else 0

                tablas_existentes.append({'tabla': tabla_completa, 'registros': total})
                print(f"   ✅ {tabla_completa:<45} → {total:,} registros")
            else:
                tablas_no_existentes.append(tabla_completa)
                print(f"   ❌ {tabla_completa:<45} → NO EXISTE")

        except Exception as e:
            tablas_no_existentes.append(tabla_completa)
            print(f"   ❌ {tabla_completa:<45} → ERROR: {str(e)[:50]}")

    # Resumen
    print("\n" + "=" * 80)
    print("📊 RESUMEN:")
    print("=" * 80)
    print(f"✅ Tablas que EXISTEN:     {len(tablas_existentes)}")
    print(f"❌ Tablas que NO EXISTEN:  {len(tablas_no_existentes)}")

    if tablas_no_existentes:
        print(f"\n⚠️  TABLAS QUE NO EXISTEN (serán omitidas por TRUNCATE):")
        for tabla in tablas_no_existentes:
            print(f"   - {tabla}")

    if tablas_existentes:
        print(f"\n✅ TABLAS QUE EXISTEN (serán truncadas):")
        total_registros = sum([t['registros'] for t in tablas_existentes])
        for tabla_info in tablas_existentes:
            print(f"   - {tabla_info['tabla']:<45} ({tabla_info['registros']:,} registros)")
        print(f"\n📊 Total de registros que se ELIMINARÁN con TRUNCATE: {total_registros:,}")

    db.close_pool()
    print("\n" + "=" * 80)

if __name__ == "__main__":
    try:
        verificar_tablas_en_bd()
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
