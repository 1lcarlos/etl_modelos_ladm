# Update area_registral_m2 en gc_predio

## Descripcion

Script SQL que actualiza el campo `area_registral_m2` en la tabla `gc_predio` de la base de datos destino (modelo v3/RIC), trayendo los valores desde el campo `area_registral_m2` de la tabla `cca_predio` en la base de datos origen (modelo CCA).

Este campo no fue migrado durante el proceso ETL original (estaba comentado en los scripts de insercion), por lo que este script permite llevar ese dato sin necesidad de volver a ejecutar toda la migracion.

**Archivo:** `update_area_registral_gc_predio.sql`

## Bases de datos involucradas

| | Origen (CCA) | Destino (v3/RIC) |
|---|---|---|
| **Host** | localhost | localhost |
| **Puerto** | 5433 | 5433 |
| **Base de datos** | ladm_col | interno_v3_acc |
| **Schema** | cca_cun25436 | cun25436 (configurable via `{schema}`) |
| **Tabla** | cca_predio | gc_predio |
| **Campo area** | area_registral_m2 | area_registral_m2 |
| **Campo predial** | numero_predial | numero_predial_nacional |
| **Campo nupre** | nupre | nupre |

## Logica del cruce

El UPDATE cruza los registros entre origen y destino usando dos campos:

1. **numero_predial** (CCA) = **numero_predial_nacional** (destino) -- campo principal
2. **nupre** (CCA) = **nupre** (destino) -- campo secundario, maneja NULLs

Si tras el update principal quedan registros sin actualizar por diferencias en el `nupre` (por ejemplo, el destino tiene `'BBK00000'` como valor por defecto mientras el origen tiene el valor real), se puede descomentar el **PASO 3b** que cruza unicamente por `numero_predial`.

## Prerequisitos

- PostgreSQL con la extension `dblink` disponible (el script la habilita automaticamente).
- La base de datos CCA (`ladm_col`) debe estar accesible desde el servidor destino en `localhost:5433`.
- El usuario de PostgreSQL debe tener permisos de lectura sobre la BD origen y escritura sobre la BD destino.

## Como ejecutar

### 1. Configurar el schema destino

Reemplazar todas las ocurrencias de `{schema}` con el schema real del destino. Por ejemplo, si el schema es `cun25436`:

```sql
-- Antes:
UPDATE {schema}.gc_predio gp ...

-- Despues:
UPDATE cun25436.gc_predio gp ...
```

Esto se puede hacer con buscar/reemplazar en cualquier editor de texto, o directamente desde la terminal:

**Windows (PowerShell):**
```powershell
(Get-Content update_area_registral_gc_predio.sql) -replace '\{schema\}', 'cun25436' | Set-Content update_area_registral_gc_predio_ejecutable.sql
```

**Linux/Mac:**
```bash
sed 's/{schema}/cun25436/g' update_area_registral_gc_predio.sql > update_area_registral_gc_predio_ejecutable.sql
```

### 2. Ajustar la conexion dblink (si es necesario)

Si la BD CCA no esta en `localhost:5433/ladm_col`, editar la cadena de conexion en el PASO 1:

```sql
'host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123'
```

### 3. Ejecutar paso a paso

Se recomienda ejecutar el script en orden, validando cada paso antes de continuar.

**Conectarse a la BD destino:**
```bash
psql -h localhost -p 5433 -d interno_v3_acc -U postgres
```

**Ejecucion recomendada (paso a paso):**

| Paso | Que hace | Accion |
|------|----------|--------|
| **PASO 1** | Crea tabla temporal `tmp_update_area_registral` con los datos del CCA via dblink | Ejecutar siempre |
| **PASO 2** | 6 consultas de verificacion previa: conteos, cruces y preview de datos | Revisar resultados antes de continuar |
| **PASO 3** | UPDATE principal usando numero_predial + nupre | Ejecutar tras validar PASO 2 |
| **PASO 3b** | UPDATE complementario solo por numero_predial (comentado por defecto) | Descomentar solo si hay registros que no cruzaron en PASO 3 |
| **PASO 4** | Verificacion posterior: conteos y distribucion de valores | Ejecutar para confirmar resultados |
| **PASO 5** | Elimina la tabla temporal | Ejecutar al finalizar |

### 4. Ejecucion completa (script completo)

Si ya se valido en un ambiente de pruebas y se quiere ejecutar todo de una vez:

```bash
psql -h localhost -p 5433 -d interno_v3_acc -U postgres -f update_area_registral_gc_predio_ejecutable.sql
```

## Verificaciones del PASO 2 (detalle)

| Consulta | Que verifica | Resultado esperado |
|----------|--------------|-------------------|
| 2.1 | Total registros extraidos del CCA con area_registral_m2 NOT NULL | Debe ser > 0 |
| 2.2 | Cruzan por numero_predial **Y** nupre | Idealmente igual a 2.1 |
| 2.3 | Cruzan solo por numero_predial | >= 2.2 (incluye los que no cruzan por nupre) |
| 2.4 | Registros destino con area_registral_m2 NULL actualmente | Cantidad de registros que podrian beneficiarse del update |
| 2.5 | Registros destino sin cruce | Predios en destino que no existen en el origen CCA |
| 2.6 | Preview de 20 registros | Comparacion visual de area_actual vs area_nueva |

**Si 2.2 es significativamente menor que 2.3**, hay registros que cruzan por `numero_predial` pero no por `nupre`. En ese caso, considerar descomentar el **PASO 3b**.

## Notas

- El script usa `DISTINCT ON (numero_predial)` al extraer del CCA para evitar duplicados que pueden surgir por los LEFT JOINs de la query original.
- El UPDATE solo modifica registros donde el origen tiene `area_registral_m2 IS NOT NULL`.
- El PASO 3b (comentado) es un fallback que actualiza solo por `numero_predial` los registros que quedaron con `area_registral_m2 IS NULL` tras el PASO 3.
- Los valores de `area_registral_m2` se expresan en metros cuadrados con precision `numeric(25,2)`.
