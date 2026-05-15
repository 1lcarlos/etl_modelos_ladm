# Bitacora de Consultas - Flujo Inverso ETL (interno_v3 → Django)

## Objetivo

Construir las sentencias SQL en sentido inverso al flujo original del ETL:

- **Flujo original**: modelo interno Django → modelo interno_v3
- **Flujo inverso**: modelo interno_v3 → modelo interno Django

El flujo original extrae datos con queries en `sql/queries/queries_modelo_interno_django/` e inserta con scripts en `sql/inserts/inserts_interno_v3/`. El flujo inverso hace lo contrario.

---

## Fuentes de Referencia

| Recurso | Descripción |
|---|---|
| `backup/backup_parcial_gc_predio.sql` | DDL del modelo Django (interno_acc, esquema cun25489). Tablas con PK `id`, secuencias por tabla, dominios con `text_code` |
| `backup/backup_parcial_destino.sql` | DDL del modelo interno_v3 (interno_v3_acc, esquema cun25489). Tablas con PK `t_id`, secuencia global `t_ili2db_seq`, dominios con `ilicode` |
| `sql/queries/queries_modelo_interno_django/` | Queries del flujo original (extracción desde Django) |
| `sql/inserts/inserts_interno_v3/` | Inserts del flujo original (escritura en interno_v3) |

---

## Decisiones Tomadas

### 1. Generación de PKs
- **Django genera sus propios IDs** usando las secuencias por tabla (`gc_predio_id_seq`, `gc_terreno_id_seq`, etc.)
- No se reutilizan los `t_id` de interno_v3 como `id` en Django
- El campo `local_id` se preserva para mantener trazabilidad entre modelos

### 2. Mapeo de Dominios
- **interno_v3** usa tablas de dominio con campo `ilicode` y PK `t_id`
- **Django** usa tablas de dominio con campo `text_code` y PK `id`
- Patron de coincidencia de 2 niveles (igual que el flujo original):
  1. Coincidencia exacta: `text_code = ilicode`
  2. Coincidencia parcial: `ilicode ILIKE '%' || text_code || '%'`
  3. Fallback con valor por defecto cuando aplica

### 3. Resolución de Foreign Keys
- Las FK entre tablas principales se resuelven mediante `local_id`
- En el query de v3 se extrae el `local_id` de las tablas referenciadas (JOIN con tablas FK)
- En el insert a Django se hace JOIN con las tablas recién insertadas usando `local_id` para obtener el nuevo `id`
- Se usa `DISTINCT ON (local_id)` para evitar duplicados en los JOINs

### 4. Campos sin equivalente directo
| Campo | Decisión |
|---|---|
| `fraccion_derecho` | No existe en `gc_derechocatastral` (v3). Se inserta `1` por defecto en Django |
| `valor_transaccion` | No existe en v3. No se mapea (queda NULL en Django) |
| `matricula_inmobiliaria` | v3 es `integer >= 1`, Django es `varchar(80)`. Se hace cast a varchar preservando el valor. Si es NULL o <= 0, se pone `'1'` |
| `estado_conservacion` | v3 lo tiene como campo fijo. No aplica al reverso (no existe en Django `gc_predio`) |
| `verificado` (interesado) | No existe en v3. Queda NULL en Django |
| `estado` (predio) | Existe en v3 como FK. No existe en Django `gc_predio`. No se mapea |
| `codigo_homologado` | Existe en v3. No existe en Django `gc_predio`. No se mapea |
| `vigencia_actualizacion_catastral` | Existe en v3. No existe en Django `gc_predio`. No se mapea |
| `area_registral_m2` | En v3 esta en `gc_predio`. En Django esta en `dlc_datosadicionaleslevantamientocatastral` (tabla separada, pendiente) |
| `rrr_gc_restriccion` | Se pasa NULL (gc_restriccion no migrada en este flujo) |

### 5. Tablas con nombre diferente entre modelos
| interno_v3 | Django | Nota |
|---|---|---|
| `gc_derechocatastral` | `gc_derecho` | Tabla principal de derechos |
| `gc_derechocatastraltipo` | `gc_derechotipo` | Tabla de dominio de tipo de derecho |
| `col_fuenteadministrativatipo` | `gc_fuenteadministrativatipo` | Tabla de dominio de tipo fuente |
| `col_documentotipo` | `gc_interesadodocumentotipo` | Tabla de dominio de tipo documento |
| `col_interesadotipo` | `gc_interesadotipo` | Tabla de dominio de tipo interesado |
| `gc_autoreconocimientoetnicotipo` | `gc_grupoetnicotipo` | Tabla de dominio grupo etnico |
| `col_uebaunit.baunit` | `col_uebaunit.unidad` | Nombre del FK al predio |
| `gc_derechocatastral.unidad` | `gc_derecho.baunit` | Nombre del FK al predio |
| `col_rrrfuente.rrr_gc_derechocatastral` | `col_rrrfuente.rrr_gc_derecho` | Nombre del FK al derecho |
| `gc_fuenteadministrativa.nombre` | `gc_fuenteadministrativa.numero_fuente` | Campo de numero/nombre fuente |

### 6. Estructura de carpetas
- Se crearon carpetas nuevas diferenciadas de las existentes:
  - `sql/queries/queries_interno_v3/` — Queries de extracción desde interno_v3
  - `sql/inserts/inserts_django_from_v3/` — Inserts de escritura en Django
- Las carpetas originales se mantienen intactas:
  - `sql/queries/queries_modelo_interno_django/` — Queries del flujo original
  - `sql/inserts/inserts_interno_v3/` — Inserts del flujo original

---

## Archivos Creados

### Queries: `sql/queries/queries_interno_v3/`

| # | Archivo | Tabla origen (v3) | JOINs de dominio |
|---|---|---|---|
| 1 | `predio.sql` | `gc_predio` | `gc_prediotipo`, `gc_categoriasuelotipo`, `gc_clasesuelotipo`, `gc_condicionprediotipo`, `gc_destinacioneconomicatipo` |
| 2 | `terreno.sql` | `gc_terreno` | Ninguno |
| 3 | `col_uebaunit_predio_terreno.sql` | `col_uebaunit` | JOIN `gc_terreno` + `gc_predio` para extraer `local_id` |
| 4 | `interesado.sql` | `gc_interesado` | `gc_estadociviltipo`, `gc_autoreconocimientoetnicotipo`, `gc_sexotipo`, `col_interesadotipo`, `col_documentotipo` |
| 5 | `agrupacion_interesados.sql` | `gc_agrupacioninteresados` | `col_grupointeresadotipo` |
| 6 | `col_miembros.sql` | `col_miembros` | JOIN `gc_interesado` + `gc_agrupacioninteresados` para extraer `local_id` |
| 7 | `fuente_administrativa.sql` | `gc_fuenteadministrativa` | `col_estadodisponibilidadtipo`, `col_fuenteadministrativatipo` |
| 8 | `col_unidad_fuente.sql` | `col_unidadfuente` | JOIN `gc_predio` + `gc_fuenteadministrativa` para extraer `local_id` |
| 9 | `extdireccion.sql` | `extdireccion` | `extdireccion_tipo_direccion`, `extdireccion_clase_via_principal`, `extdireccion_sector_ciudad`, `extdireccion_sector_predio`, JOIN `gc_predio` |
| 10 | `derecho.sql` | `gc_derechocatastral` | `gc_derechocatastraltipo`, JOIN `gc_predio` + `gc_interesado` + `gc_agrupacioninteresados` para `local_id` |
| 11 | `col_rrrfuente.sql` | `col_rrrfuente` | JOIN `gc_fuenteadministrativa` + `gc_derechocatastral` para `local_id` |

### Inserts: `sql/inserts/inserts_django_from_v3/`

| # | Archivo | Tabla destino (Django) | Dominios mapeados (ilicode → text_code) |
|---|---|---|---|
| 01 | `01_insert_predios.sql` | `gc_predio` | `gc_prediotipo`, `gc_categoriasuelotipo`, `gc_clasesuelotipo`, `gc_condicionprediotipo`, `gc_destinacioneconomicatipo` |
| 02 | `02_insert_terrenos.sql` | `gc_terreno` | Ninguno (mapeo directo) |
| 03 | `03_insert_col_uebaunit_predio_terreno.sql` | `col_uebaunit` | FK por `local_id` de `gc_terreno` y `gc_predio` |
| 04 | `04_insert_interesados.sql` | `gc_interesado` | `gc_estadociviltipo`, `gc_grupoetnicotipo`, `gc_sexotipo`, `gc_interesadotipo`, `gc_interesadodocumentotipo` |
| 05 | `05_insert_agrupaciones.sql` | `gc_agrupacioninteresados` | `col_grupointeresadotipo` |
| 06 | `06_insert_col_miembros.sql` | `col_miembros` | FK por `local_id` con `DISTINCT ON` |
| 07 | `07_insert_fuente_administrativa.sql` | `gc_fuenteadministrativa` | `col_estadodisponibilidadtipo`, `gc_fuenteadministrativatipo` |
| 08 | `08_insert_col_unidadfuente.sql` | `col_unidadfuente` | FK por `local_id` de `gc_predio` y `gc_fuenteadministrativa` |
| 09 | `09_insert_extdireccion.sql` | `extdireccion` | `extdireccion_tipo_direccion`, `extdireccion_clase_via_principal`, `extdireccion_sector_ciudad`, `extdireccion_sector_predio`. FK predio por `local_id` |
| 10 | `10_insert_derecho.sql` | `gc_derecho` | `gc_derechotipo`. FK predio, interesado y agrupacion por `local_id`. `fraccion_derecho = 1` |
| 11 | `11_insert_col_rrrfuente.sql` | `col_rrrfuente` | FK por `local_id` de `gc_fuenteadministrativa` y `gc_derecho`. `rrr_gc_restriccion = NULL` |

---

## Orden de Ejecución Sugerido

El orden de los inserts respeta las dependencias de FK:

```
01_insert_predios.sql                       -- gc_predio (sin dependencias)
02_insert_terrenos.sql                      -- gc_terreno (sin dependencias)
03_insert_col_uebaunit_predio_terreno.sql   -- depende de: gc_predio, gc_terreno
04_insert_interesados.sql                   -- gc_interesado (sin dependencias)
05_insert_agrupaciones.sql                  -- gc_agrupacioninteresados (sin dependencias)
06_insert_col_miembros.sql                  -- depende de: gc_interesado, gc_agrupacioninteresados
07_insert_fuente_administrativa.sql         -- gc_fuenteadministrativa (sin dependencias)
08_insert_col_unidadfuente.sql              -- depende de: gc_predio, gc_fuenteadministrativa
09_insert_extdireccion.sql                  -- depende de: gc_predio
10_insert_derecho.sql                       -- depende de: gc_predio, gc_interesado, gc_agrupacioninteresados
11_insert_col_rrrfuente.sql                 -- depende de: gc_fuenteadministrativa, gc_derecho
```

---

## Tablas Pendientes

Las siguientes tablas del flujo original aun no tienen su equivalente inverso:

- `gc_caracteristicasunidadconstruccion` / `gc_unidadconstruccion` / `col_uebaunit_predio_unidadconstruccion`
- `gc_calificacionconvencional` / `gc_calificacionnoconvencional` / `gc_grupocalificacion`
- `gc_lindero` / `gc_puntolindero` / `gc_puntocontrol`
- `col_masccl` / `col_menosccl` / `col_puntoccl`
- `gc_estructuraavaluo` / `gc_estructuraalertapredio`
- `cc_vereda`, `cc_corregimiento`, `cc_centropoblado`, `cc_sectorrural`, `cc_nomenclaturavial`, etc. (cartografia)
- `gc_construccion` / `gc_datosphcondominio`
- `dlc_datosadicionaleslevantamientocatastral` (contiene `area_registral_m2`)
- `gc_restriccion` (necesaria para completar `col_rrrfuente.rrr_gc_restriccion`)
