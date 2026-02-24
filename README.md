# ETL PostgreSQL - Migracion LADM-COL

Sistema ETL para migrar datos catastrales entre modelos PostgreSQL siguiendo el estandar LADM-COL. Soporta migracion entre los modelos CCA, modelo interno Django, RIC y SINIC, ejecutando queries de extraccion, creacion de tablas temporales e inserts ordenados por dependencias.

## Arquitectura

```
etl/
├── main.py                  # Punto de entrada CLI
├── gui.py                   # Interfaz grafica (Tkinter)
├── run_gui.bat              # Lanzador de la GUI en Windows
├── requirements.txt         # Dependencias Python
├── .env.example             # Plantilla de variables de entorno
├── etl_config.example.json  # Plantilla de configuracion para despliegue
├── config/
│   └── config.py            # Configuracion centralizada (lee .env)
├── src/
│   ├── database_manager.py  # Conexiones, pools, TRUNCATE, tablas temporales
│   ├── etl_processor.py     # Orquestador del flujo ETL
│   └── logger.py            # Sistema de logging con rotacion
└── sql/
    ├── queries/             # SELECT de extraccion por modelo
    │   ├── queries_cca/
    │   ├── queries_modelo_interno_django/
    │   └── queries_sinic/
    ├── inserts/             # INSERT de carga por modelo
    │   ├── inserts_cca/
    │   ├── insertes_modelo_interno_django/
    │   ├── inserts_interno_v3/
    │   ├── inserts_ric_finales/
    │   └── sinic_inserts/
    ├── insert_order*.txt    # Orden de ejecucion por modelo
    └── verificacion/        # Scripts SQL de verificacion post-migracion
```

## Requisitos previos

- **Python** 3.8 o superior
- **PostgreSQL** 12+ en origen y destino
- Extensiones PostgreSQL en la base destino:
  - `postgis` (geometrias)
  - `dblink` (conexion remota entre bases, si aplica)
- Acceso de red entre la maquina que ejecuta el ETL y ambos servidores PostgreSQL

## Instalacion

```bash
# 1. Clonar el repositorio
git clone <url-del-repositorio>
cd etl

# 2. Crear entorno virtual (recomendado)
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Linux/Mac

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar variables de entorno
copy .env.example .env       # Windows
# cp .env.example .env       # Linux/Mac
# Editar .env con las credenciales reales
```

## Configuracion

### Variables de entorno (`.env`)

El archivo `.env` contiene toda la configuracion del ETL. Copie `.env.example` como punto de partida.

| Variable | Descripcion | Ejemplo |
|----------|-------------|---------|
| `DB_ORIGEN_HOST` | Host de la base de datos origen | `localhost` |
| `DB_ORIGEN_PORT` | Puerto de la base origen | `5433` |
| `DB_ORIGEN_DATABASE` | Nombre de la base origen | `interno_acc` |
| `DB_ORIGEN_USER` | Usuario PostgreSQL origen | `postgres` |
| `DB_ORIGEN_PASSWORD` | Password del usuario origen | *(requerido)* |
| `DB_DESTINO_HOST` | Host de la base de datos destino | `localhost` |
| `DB_DESTINO_PORT` | Puerto de la base destino | `5433` |
| `DB_DESTINO_DATABASE` | Nombre de la base destino | `interno_v3_acc` |
| `DB_DESTINO_USER` | Usuario PostgreSQL destino | `postgres` |
| `DB_DESTINO_PASSWORD` | Password del usuario destino | *(requerido)* |
| `SCHEMAS` | Esquemas a procesar (separados por coma) | `cun25489` |
| `BATCH_SIZE` | Registros por lote | `1000` |
| `CONNECTION_POOL_SIZE` | Conexiones simultaneas al pool | `5` |
| `TRUNCATE_ENABLED` | Limpiar tablas destino antes de insertar | `true` |
| `TRUNCATE_REQUIRE_CONFIRMATION` | Pedir confirmacion antes de TRUNCATE | `true` |
| `TRUNCATE_USE_CASCADE` | Usar CASCADE en TRUNCATE | `true` |

### Archivo de despliegue (`etl_config.json`)

Alternativa al `.env` para despliegues puntuales donde se necesita especificar rutas absolutas a los archivos SQL y conexiones directas. Copie `etl_config.example.json` como plantilla. La GUI (`gui.py`) utiliza este archivo para cargar la configuracion.

### TRUNCATE

El ETL limpia las tablas destino antes de cada migracion para evitar duplicados. El comportamiento se controla con estas variables:

- **`TRUNCATE_ENABLED=true`**: Activa la limpieza. Poner `false` para inserts incrementales (riesgo de duplicados).
- **`TRUNCATE_REQUIRE_CONFIRMATION=true`**: Muestra la lista de tablas y pide escribir `SI` antes de truncar. Poner `false` para ejecuciones automatizadas.
- **`TRUNCATE_USE_CASCADE=true`**: Usa `TRUNCATE CASCADE` para manejar foreign keys. Recomendado siempre `true`.

## Uso

### Linea de comandos (CLI)

```bash
# Modo interactivo (pide confirmacion antes de TRUNCATE)
python main.py

# Modo automatico (sin confirmacion, ideal para tareas programadas)
python main.py --no-confirm

# Simulacion sin cambios reales
python main.py --dry-run

# Procesar esquemas especificos
python main.py --schemas cun25154,cun25489

# Ver configuracion actual
python main.py --config

# Ver version
python main.py --version
```

Para tareas programadas (Task Scheduler / cron):
```bash
python main.py --no-confirm >> logs/cron.log 2>&1
```

### Interfaz grafica (GUI)

```bash
# Opcion 1: Usar el lanzador
run_gui.bat

# Opcion 2: Ejecutar directamente
python gui.py
```

La GUI permite configurar conexiones, seleccionar esquemas, elegir el conjunto de SQL (queries/inserts) y ejecutar el ETL visualmente. Lee la configuracion desde `etl_config.json`.

## Estructura SQL

### Queries (`sql/queries/`)

Archivos SQL de tipo `SELECT` que extraen datos de la base origen. Se ejecutan contra la base origen y sus resultados se almacenan en tablas temporales en la base destino. Estan organizados por modelo destino:

- `queries_cca/` - Queries para modelo CCA
- `queries_modelo_interno_django/` - Queries para modelo interno Django
- `queries_sinic/` - Queries para modelo SINIC

### Inserts (`sql/inserts/`)

Archivos SQL de tipo `INSERT INTO ... SELECT` que toman datos de las tablas temporales y los insertan en las tablas destino, aplicando transformaciones (mapeo de dominios, conversion de tipos, geometrias). Estan organizados por modelo:

- `inserts_cca/` - Inserts para CCA
- `insertes_modelo_interno_django/` - Inserts para modelo interno Django (22 tablas)
- `inserts_interno_v3/` - Inserts para modelo interno v3
- `inserts_ric_finales/` - Inserts para modelo RIC
- `sinic_inserts/` - Inserts para modelo SINIC

### Orden de ejecucion (`sql/insert_order*.txt`)

Archivos de texto que definen el orden en que se ejecutan los inserts, respetando dependencias entre tablas (foreign keys). Hay un archivo por modelo. El ETL lee el archivo correspondiente y ejecuta los inserts en ese orden.

### Verificacion (`sql/verificacion/`)

Scripts SQL para validar la migracion una vez completada:

| Archivo | Nivel | Descripcion |
|---------|-------|-------------|
| `00_configuracion_dblink.sql` | 0 | Configura dblink para conectar origen y destino |
| `01_verificacion_conteo_registros.sql` | 1 | Compara conteos de registros entre bases |
| `02_verificacion_mapeo_dominios.sql` | 2 | Valida que los dominios se mapearon correctamente |
| `03_verificacion_transformaciones.sql` | 3 | Verifica transformaciones especiales (fraccion, booleanos) |
| `04_verificacion_integridad_referencial.sql` | 4 | Comprueba foreign keys e integridad |
| `05_verificacion_muestreo_campos.sql` | 5 | Muestreo aleatorio de campos para inspeccion manual |

Ver `sql/verificacion/README.md` para instrucciones detalladas.

## Flujo del proceso

```
1. Conectar a bases de datos (origen y destino)
         |
2. Analizar insert_order.txt -> identificar tablas destino
         |
3. TRUNCATE tablas destino (si esta habilitado)
   - Muestra advertencia y pide confirmacion
   - Ejecuta TRUNCATE CASCADE
   - Omite tablas que no existen
         |
4. Ejecutar queries de extraccion (origen -> tablas temporales en destino)
   - Queries con datos: crea tabla temporal con registros
   - Queries sin datos: crea tabla temporal vacia (no interrumpe el proceso)
         |
5. Validar dependencias entre tablas temporales e inserts
         |
6. Ejecutar inserts en orden definido
   - Mapeo de dominios (ilicode -> text_code -> FK id)
   - Transformaciones especiales por tabla
   - Si un insert falla, continua con el siguiente
         |
7. Limpiar tablas temporales
         |
8. Generar reporte final con estadisticas
   - Registros insertados por tabla
   - Inserts exitosos vs fallidos
   - Log detallado en logs/
```

## Verificacion post-migracion

Despues de ejecutar el ETL, se recomienda ejecutar los scripts de verificacion en orden (00 a 05) contra la base destino. Estos scripts usan `dblink` para comparar los datos migrados contra la fuente original.

```bash
# Ejecutar desde psql conectado a la base destino
psql -h localhost -p 5433 -U postgres -d nombre_base_destino

# Dentro de psql, ejecutar cada script en orden:
\i sql/verificacion/00_configuracion_dblink.sql
\i sql/verificacion/01_verificacion_conteo_registros.sql
-- ... y asi sucesivamente
```

Consulte `sql/verificacion/README.md` para detalles sobre cada nivel de verificacion.
