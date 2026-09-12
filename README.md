# ETL PostgreSQL - Migracion LADM-COL

Sistema ETL para migrar datos catastrales entre modelos PostgreSQL siguiendo el estandar LADM-COL. Soporta migracion entre los modelos CCA, modelo interno Django, modelo interno v3, RIC y SINIC, ejecutando queries de extraccion, creacion de tablas temporales e inserts ordenados por dependencias.

## Arquitectura

```
etl/
├── main.py                  # Punto de entrada CLI
├── gui.py                   # Interfaz grafica (Tkinter): flujos, verificacion, simulacion y ejecucion
├── run_gui.bat              # Lanzador de la GUI en Windows
├── requirements.txt         # Dependencias Python
├── .env.example             # Plantilla de variables de entorno
├── etl_config.example.json  # Plantilla de configuracion para despliegue
├── config/
│   ├── config.py            # Configuracion centralizada (lee .env)
│   └── flujos.json          # Flujos de migracion que ofrece la GUI (queries + inserts + orden)
├── src/
│   ├── database_manager.py  # Conexiones, pools, TRUNCATE, tablas temporales, transaccion unica (simulacion)
│   ├── etl_processor.py     # Orquestador del flujo ETL (eventos de avance, detencion, simulacion)
│   ├── flujos.py            # Lectura del orden y verificacion de flujos sin conectarse a la BD
│   └── logger.py            # Sistema de logging a archivo y consola
└── sql/
    ├── queries/             # SELECT de extraccion por flujo
    ├── inserts/             # INSERT de carga por flujo
    ├── insert_order*.txt    # Orden de ejecucion por flujo
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

El archivo `.env` contiene la configuracion de la CLI (`main.py`). Copie `.env.example` como punto de partida.

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

### Archivo de la GUI (`etl_config.json`)

La GUI (`gui.py`) guarda y carga su configuracion en `etl_config.json` (esta en `.gitignore` porque contiene contrasenas). Se guarda al ejecutar, con Ctrl+S y al cerrar. Copie `etl_config.example.json` como plantilla para un despliegue nuevo.

| Seccion | Campo | Descripcion |
|---------|-------|-------------|
| *(raiz)* | `flujo` | Id del flujo de `config/flujos.json` (vacio = rutas manuales) |
| `origen` | `host`, `port`, `database`, `user`, `password` | Conexion a BD origen |
| `destino` | `host`, `port`, `database`, `user`, `password` | Conexion a BD destino |
| *(raiz)* | `schema` | Esquema para modo unico (ej: `"cun25436"`) |
| `paths` | `queries`, `inserts`, `order_file`, `logs` | Rutas absolutas a carpetas SQL y logs |
| `options` | `truncate_enabled`, `truncate_cascade` | Vaciar tablas destino antes de cargar |
| `options` | `dry_run` | `true` = modo simulacion (ROLLBACK al final) |
| `options` | `schema_mode` | Modo de esquemas: `"single"` o `"multi"` |
| `options` | `batch_size`, `pool_size` | Rendimiento (no se editan desde la GUI) |
| *(raiz)* | `schemas` | Lista de esquemas para modo multi (ej: `["cun25436", "cun25489"]`) |

### Flujos de migracion (`config/flujos.json`)

Un flujo es la combinacion de una carpeta de queries, una carpeta de inserts y un archivo de orden. La GUI los muestra en el selector **Flujo**; asi no hay que elegir las tres rutas a mano ni copiar el orden sobre `insert_order.txt`.

```json
{
    "id": "django_a_v3_v2",
    "nombre": "Django → interno v3 ACC (v2)",
    "descripcion": "Texto que se muestra debajo del selector",
    "queries": "sql/queries/queries_django_a_v3_v2",
    "inserts": "sql/inserts/inserts_django_a_v3_v2",
    "orden": "sql/insert_order_django_a_v3_v2.txt",
    "vigente": true
}
```

- Las rutas son relativas a la carpeta del proyecto.
- `orden` vacio: los inserts se ejecutan en orden alfabetico.
- `vigente: false`: el flujo aparece al final con la marca *(historico)*.
- Para agregar un flujo: **Herramientas > Editar lista de flujos**, guardar y luego **Herramientas > Recargar lista de flujos**.

| Flujo | Queries | Inserts | Orden |
|-------|---------|---------|-------|
| Django → interno v3 ACC (v2) | `queries_django_a_v3_v2` | `inserts_django_a_v3_v2` | `insert_order_django_a_v3_v2.txt` |
| Interno v3 → SINIC (novedades) | `queries_interno_v3_para_sinic` | `inserts_interno_v3_a_sinic` | `insert_order_novedades_sinic.txt` |
| Interno v3 → SINIC (mutacion de primera) | `queries_interno_v3_para_sinic` | `inserts_interno_v3_a_sinic` | `insert_order_mutacion_primera_sinic.txt` |
| CCA → modelo interno Django | `queries_cca` | `insertes_modelo_interno_django` | `insert_order_modelo_interno_django.txt` |
| Modelo interno → SINIC (version inicial) | `queries_sinic` | `sinic_inserts` | `insert_order_sinic.txt` |
| Modelo interno → RIC | `usados_ric` | `inserts_ric_finales` | `insert_order_ric.txt` |
| Interno v3 → Django (flujo inverso) | `queries_interno_v3` | `inserts_django_from_v3` | *(alfabetico)* |
| Django → interno v3 (v1) *(historico)* | `queries_modelo_interno_django` | `inserts_interno_v3` | `insert_order_interno_v3.txt` |
| Django → interno v3 ACC inicial *(historico)* | `queries_django_a_v3` | `inserts_django_a_v3` | `insert_order_interno_acc_v3.txt` |

### TRUNCATE

El ETL limpia las tablas destino antes de cada migracion para evitar duplicados. Las tablas se identifican en los `INSERT INTO` de los archivos del orden; los nombres escritos entre comillas (por ejemplo `"etl_control"."incidencia"`) no se truncan. En la CLI el comportamiento se controla con estas variables:

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

# Simulacion: ejecuta todo en una transaccion y la deshace al final (no guarda cambios)
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

Todo lo necesario para una corrida esta en una sola pantalla:

| Seccion | Descripcion |
|---------|-------------|
| **Flujo** | Selector de flujos de `config/flujos.json`. Debajo muestra cuantas queries e inserts tiene y si la verificacion encontro problemas. **Rutas…** permite rutas propias (opcion *Personalizado*). |
| **Origen / Destino** | Resumen de cada conexion con su estado (conectado, sin conexion, sin probar). **Editar…** abre el formulario (con prueba de conexion) y **Probar** la verifica sin bloquear la ventana. Las conexiones se prueban al abrir la GUI. |
| **Esquema** | *Unico* (se escribe el nombre) o *Varios* (**Elegir…** consulta la BD origen, con filtro y marcar/desmarcar). |
| **Modo** | *Real* guarda los cambios; *Simulacion* ejecuta todo y lo deshace al final. Opciones de TRUNCATE y CASCADE. |
| **Ejecutar / Detener** | Barra de avance por archivo, tiempo transcurrido y archivo en curso. |
| **Resultados** | Una fila por query, TRUNCATE e insert con filas, estado y tiempo. *Solo errores* filtra; doble clic muestra el error completo y permite abrir el archivo SQL. |
| **Verificacion** | Revision del flujo sin conectarse: inserts en orden, archivos que faltan, desactivados, archivos que no estan en el orden, tablas `tmp_` sin query, archivos con `COMMIT` y tablas que se vaciaran. Doble clic abre el archivo. |
| **Log** | El log del ETL en tiempo real (el mismo que se guarda en `logs/`). |

Flujo de trabajo recomendado:

1. Elegir el **Flujo** y revisar la pestana **Verificacion** (se actualiza sola).
2. Confirmar que Origen y Destino aparecen como *conectado*.
3. Elegir el esquema (o varios).
4. Ejecutar primero en modo **Simulacion**: muestra que inserts fallarian y cuantas filas cargaria cada uno, sin guardar nada.
5. Corregir los errores y ejecutar en modo **Real**. Antes de empezar se muestra un resumen con la lista de tablas que se vaciaran.

Detalles:

- **Simulacion**: cada esquema corre en una sola transaccion; cada archivo usa un `SAVEPOINT` para que un error no detenga los siguientes, y al final se hace `ROLLBACK`. Mientras dura, las tablas destino quedan bloqueadas para otros usuarios y las secuencias (`t_ili2db_seq`) avanzan. Los archivos con `BEGIN`/`COMMIT`/`ROLLBACK` propios no se ejecutan en simulacion (se reportan como error).
- **Detener**: cancela la sentencia en curso en PostgreSQL y no inicia archivos nuevos. En modo real, los archivos que ya terminaron quedan guardados; en simulacion se deshace todo.
- **Varios esquemas**: se procesan en secuencia. Si uno falla, la GUI pregunta si se continua con los restantes.

## Estructura SQL

### Queries (`sql/queries/`)

Archivos SQL de tipo `SELECT` que extraen datos de la base origen. Se ejecutan contra la base origen y sus resultados se almacenan en tablas temporales `tmp_<nombre_archivo>` en la base destino.

### Inserts (`sql/inserts/`)

Archivos SQL de tipo `INSERT INTO ... SELECT` que toman datos de las tablas temporales y los insertan en las tablas destino, aplicando transformaciones (mapeo de dominios, conversion de tipos, geometrias).

### Orden de ejecucion (`sql/insert_order*.txt`)

Archivos de texto que definen el orden en que se ejecutan los inserts, respetando dependencias entre tablas (foreign keys). Un archivo por linea; las lineas que empiezan con `#` son comentarios y las que empiezan con `-` son archivos desactivados.

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
2. Para cada esquema (1..N):
   |
   2a. Notificar progreso: "Esquema X/N: nombre"
       (en simulacion: abrir una transaccion unica en el destino)
         |
   2b. Analizar el archivo de orden -> identificar tablas destino
         |
   2c. TRUNCATE tablas destino (si esta habilitado)
       - Muestra advertencia y pide confirmacion (CLI)
       - Ejecuta TRUNCATE CASCADE
       - Omite tablas que no existen
         |
   2d. Ejecutar queries de extraccion (origen -> tablas temporales en destino)
       - Queries con datos: crea tabla temporal con registros
       - Queries sin datos: crea tabla temporal vacia (no interrumpe el proceso)
         |
   2e. Validar dependencias entre tablas temporales e inserts
         |
   2f. Ejecutar inserts en orden definido
       - Mapeo de dominios (ilicode -> text_code -> FK id)
       - Transformaciones especiales por tabla
       - Si un insert falla, continua con el siguiente
         |
   2g. Limpiar tablas temporales
       (en simulacion: ROLLBACK de todo el esquema)
         |
   2h. Si el esquema fallo y hay mas esquemas pendientes:
       - GUI: pregunta al usuario si desea continuar
       - CLI: continua automaticamente
         |
3. Generar reporte final con estadisticas
   - Esquemas exitosos vs fallidos
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
