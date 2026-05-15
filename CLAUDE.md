# CLAUDE.md

Este archivo proporciona orientación a Claude Code (claude.ai/code) al trabajar con el código de este repositorio.

## Referencias Clave

- **README.md** — Documentación completa del proyecto: comandos, configuración (.env y etl_config.json), estructura SQL, flujo del proceso, uso CLI/GUI, verificación post-migración. Consultar siempre que se necesite contexto detallado del ETL.
- **backup/backup_parcial_gc_predio.sql** — Backup parcial de la base de datos origen `interno_acc` (modelo interno Django, esquema `cun25489`). Contiene el DDL completo (CREATE TABLE, secuencias, constraints, índices). Consultar para conocer la estructura de tablas, tipos de columnas, relaciones FK y constraints de la BD origen.
- **backup/backup_parcial_destino.sql** — Backup parcial de la base de datos destino `interno_v3_acc` (modelo interno v3, esquema `cun25489`). Contiene el DDL completo de las tablas destino. Consultar para conocer la estructura de la BD destino donde el ETL escribe los datos.

## Comandos Rápidos

```bash
pip install -r requirements.txt         # Instalar dependencias
python main.py                          # ETL interactivo
python main.py --no-confirm             # ETL automático
python main.py --dry-run                # Simulación
python main.py --schemas cun25154,cun25489  # Esquemas específicos
python gui.py                           # Interfaz gráfica
```

No existe suite de pruebas.

## Arquitectura

**Puntos de entrada**: `main.py` (CLI) y `gui.py` (GUI Tkinter) — ambos invocan a `ETLProcessor`.

**Módulos principales** (en `src/`):
- `etl_processor.py` — Orquestador: por cada esquema ejecuta TRUNCATE → consultas de extracción → crear tablas temporales → validar dependencias → ejecutar inserts → limpieza
- `database_manager.py` — Pool de conexiones PostgreSQL (`psycopg2.pool.ThreadedConnectionPool`), ejecución de queries/inserts, gestión de tablas temporales
- `logger.py` — Logging a archivo + consola con seguimiento de estadísticas

**Configuración** (`config/config.py`): carga `.env` mediante `python-dotenv`. Copiar `.env.example` a `.env` antes de la primera ejecución.

## Convenciones SQL

Todos los archivos SQL usan `{schema}` como placeholder, reemplazado en tiempo de ejecución.

**Patrones de transformación recurrentes:**
- **Mapeo de dominios**: CCA `ilicode` → FK destino `id` mediante COALESCE de 2 niveles (coincidencia exacta, luego fallback con ILIKE)
- **Geometría**: `ST_Force3D(ST_Multi(ST_MakeValid(geom)))` → MultiPolygonZ SRID 9377
- **departamento/municipio**: separación desde un solo código mediante `SUBSTRING(1,2)` y `SUBSTRING(3,3)`
- **tiene_fmi**: `'Si'`/`'No'` → `true`/`false`
- **fraccion_derecho**: CCA 0–100 → Django 0–1 (dividir entre 100)
- **calificacion**: 1 fila CCA se desnormaliza en 5 grupos + hasta 13 objetos
