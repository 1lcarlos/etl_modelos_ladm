# Sistema de Verificacion Post-Migracion ETL (CCA -> Django)

## Descripcion General

Este sistema permite verificar la integridad, completitud y correctitud de los datos migrados desde el modelo CCA (Captura Campo) hacia el modelo interno creado en Django.

La migracion involucra **22 tablas** que fueron trasladadas desde una base de datos PostgreSQL origen (modelo CCA) hacia una base de datos destino (modelo Django). Durante la migracion se aplicaron transformaciones como division de campos, conversion de dominios, normalizacion de estructuras y transformaciones geometricas.

El sistema consiste en **6 archivos SQL independientes** organizados por niveles de verificacion, diseñados para ejecutarse directamente en pgAdmin o psql sin depender del ETL.

---

## Arquitectura

```
Base ORIGEN (CCA)                    Base DESTINO (Django)
PostgreSQL puerto 5433               PostgreSQL puerto 5432
DB: ladm_col                         DB: actualizacion
Esquema: cun25436                    Esquema: cun25436

  cca_predio ─────────────────────>    gc_predio
  cca_terreno ────────────────────>    gc_terreno
  extdireccion ───────────────────>    extdireccion
  cca_construccion ───────────────>    gc_construccion
  cca_interesado ─────────────────>    gc_interesado
  cca_derecho ────────────────────>    gc_derecho
  ...22 tablas en total               ...
```

Todas las queries se ejecutan **desde la base DESTINO** y acceden a la base ORIGEN mediante `dblink`.

---

## Estructura de Archivos

```
sql/verificacion/
├── 00_configuracion_dblink.sql                -- Setup y test de conectividad
├── 01_verificacion_conteo_registros.sql       -- Nivel 1: Conteos origen vs destino
├── 02_verificacion_mapeo_dominios.sql         -- Nivel 2: Dominios ilicode -> text_code
├── 03_verificacion_transformaciones.sql       -- Nivel 3: Transformaciones correctas
├── 04_verificacion_integridad_referencial.sql -- Nivel 4: FKs validas
├── 05_verificacion_muestreo_campos.sql        -- Nivel 5: Comparacion campo a campo
└── README.md                                  -- Este archivo
```

---

## Prerequisitos

1. **PostgreSQL** con extension `dblink` disponible
2. **Acceso a ambas bases de datos** desde la misma maquina:
   - ORIGEN: `host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123`
   - DESTINO: `host=localhost port=5432 dbname=actualizacion`
3. **pgAdmin 4** o **psql** conectado a la base DESTINO (puerto 5432)
4. La migracion ETL debe haberse ejecutado previamente (las 22 tablas deben existir en el esquema `cun25436` de ambas bases)

---

## Ejecucion Paso a Paso

### Paso 1: Configurar dblink (archivo 00)

Abrir `00_configuracion_dblink.sql` en pgAdmin conectado a la base **actualizacion** (puerto 5432) y ejecutar.

**Que hace:**
- Crea la extension `dblink` si no existe
- Ejecuta una query de prueba para leer `cca_predio` desde la base origen
- Muestra un resumen de conectividad con conteos de ambas bases

**Resultado esperado:**

| base | registros | estado |
|------|-----------|--------|
| ORIGEN (CCA - puerto 5433) | (numero) | OK |
| DESTINO (Django - puerto 5432) | (numero) | OK |

**Si falla:** Verificar que la base origen (puerto 5433) este activa y que las credenciales sean correctas.

---

### Paso 2: Verificar conteos (archivo 01)

Ejecutar `01_verificacion_conteo_registros.sql`.

**Que hace:**
Produce una tabla resumen con las 22 migraciones comparando cuantos registros deberian haberse migrado (origen) contra cuantos hay realmente en destino.

**Resultado:**

| tabla_destino | count_origen | count_destino | diferencia | estado |
|---------------|-------------|---------------|------------|--------|
| 01_gc_predio | 1500 | 1500 | 0 | OK |
| 02_gc_terreno | 1200 | 1200 | 0 | OK |
| ... | ... | ... | ... | ... |

**Interpretacion:**
- `OK`: Conteos coinciden exactamente
- `FALLO`: Hay menos registros en destino que en origen (se perdieron registros)
- `EXCESO`: Hay mas registros en destino que en origen (posibles duplicados)

**Casos especiales que el conteo maneja:**
- `gc_predio`: Solo cuenta predios con `numero_predial`, `departamento` y `municipio` no nulos (misma condicion WHERE del insert)
- `gc_terreno`: Solo cuenta terrenos con geometria no nula
- `cuc_grupocalificacion`: Multiplica origen por 5 (cada calificacion CCA genera 5 grupos: Estructura, Acabados, Banio, Cocina, Cerchas)
- `cuc_objetoconstruccion`: Suma componentes no NULL por fila (hasta 13: armazon, muros, cubierta, fachada, etc.)
- `cuc_calificacionnoconvencional`: Solo cuenta registros con `tipo_anexo` no nulo

---

### Paso 3: Verificar mapeo de dominios (archivo 02)

Ejecutar `02_verificacion_mapeo_dominios.sql`.

**Que hace:**
Verifica que los valores de dominio del modelo CCA (`ilicode`) se hayan mapeado correctamente a los valores del modelo Django (`text_code` -> `id` de la tabla de dominio). Cubre **30 campos de dominio** en 13 tablas.

**Resultado por cada campo de dominio:**

| tabla | campo_dominio | total_con_valor_origen | mapeados_ok | nulls_inesperados | estado |
|-------|--------------|----------------------|-------------|-------------------|--------|
| gc_predio | categoria_suelo | 1500 | 1500 | 0 | OK |
| gc_predio | clase_suelo | 1500 | 1500 | 0 | OK |
| gc_interesado | grupo_etnico | 800 | 800 | 0 | OK |

**Interpretacion:**
- `nulls_inesperados > 0`: Hay valores de dominio en el origen que no encontraron equivalente en destino (el COALESCE fallo). Esto indica que falta un valor en la tabla de dominio Django o que el `ilicode` del CCA no coincide con ningun `text_code`.

**Tablas y campos verificados:**

| Tabla destino | Campos de dominio | Tabla dominio Django |
|---------------|-------------------|---------------------|
| gc_predio | categoria_suelo, clase_suelo, condicion_predio, destinacion_economica, tipo | gc_categoriasuelotipo, gc_clasesuelotipo, gc_condicionprediotipo, gc_destinacioneconomicatipo, gc_prediotipo |
| extdireccion | tipo_direccion, clase_via_principal, sector_ciudad, sector_predio | extdireccion_tipo_direccion, extdireccion_clase_via_principal, extdireccion_sector_ciudad, extdireccion_sector_predio |
| gc_construccion | tipo_construccion, tipo_dominio | gc_construcciontipo, gc_dominioconstrucciontipo |
| gc_caracteristicasuc | tipo_construccion, tipo_dominio, tipo_planta, tipo_unidad_construccion, uso | gc_construcciontipo, gc_dominioconstrucciontipo, gc_construccionplantatipo, gc_unidadconstrucciontipo, gc_usouconstipo |
| gc_interesado | tipo, tipo_documento, sexo, grupo_etnico, estado_civil | gc_interesadotipo, gc_interesadodocumentotipo, gc_sexotipo, gc_grupoetnicotipo, gc_estadociviltipo |
| gc_agrupacioninteresados | tipo | col_grupointeresadotipo |
| gc_derecho | tipo | gc_derechotipo |
| gc_fuenteadministrativa | tipo | gc_fuenteadministrativatipo |
| gc_estructuranovedad | tipo_novedad | gc_estructuranovedadnumeropredial_tipo_novedad |
| cuc_calificacionconv | tipo_calificar | cuc_calificartipo |
| cuc_grupocalificacion | clase_calificacion, conservacion | cuc_clasecalificaciontipo, cuc_estadoconservaciontipo |
| cuc_objetoconstruccion | tipo_objeto_construccion | cuc_objetoconstrucciontipo |
| cuc_calificacionnoconv | tipo_anexo | cuc_anexotipo |

Incluye una query adicional que lista los valores de `grupo_etnico` en el origen para verificar los mapeos especiales:
- `Rrom` -> `Gitano`
- `Negro_Afrocolombiano` -> `Afrocolombiano`
- `Palenquero` -> `Palanquero`

---

### Paso 4: Verificar transformaciones (archivo 03)

Ejecutar `03_verificacion_transformaciones.sql`.

**Que hace:**
Verifica 8 transformaciones criticas que se aplicaron durante la migracion.

#### 3.1 - Departamento / Municipio
El campo `departamento_municipio` del CCA (5 digitos) se dividio en:
- `departamento`: primeros 2 caracteres
- `municipio`: ultimos 3 caracteres

Verifica que las longitudes sean correctas y que la concatenacion coincida con el valor original.

#### 3.2 - tiene_fmi (boolean)
El CCA almacena `tiene_fmi` como referencia a una tabla de dominio booleano (`ilicode`: `'Si'`/`'No'`). Django lo almacena como `boolean` (`true`/`false`). Verifica la conversion registro por registro.

#### 3.3 - fraccion_derecho / 100
El CCA almacena `fraccion_derecho` en rango 0-100. Django lo almacena en rango 0-1. Verifica que `destino = origen / 100` para todos los registros. Incluye query de detalle que muestra las discrepancias si existen.

#### 3.4 - Geometrias
Verifica para `gc_terreno`, `gc_construccion` y `gc_unidadconstruccion`:
- `ST_SRID` = 9377
- `ST_NDims` = 3 (coordenadas XYZ)
- `ST_GeometryType` es `MultiPolygon` o `MultiPolygonZ`

#### 3.5 - Split de nombre del reconocedor
En el CCA, el nombre del reconocedor/usuario es un campo unico. En Django se divide en `primer_nombre_reconocedor`, `segundo_nombre_reconocedor`, `primer_apellido_reconocedor`, `segundo_apellido_reconocedor`. Verifica que los campos no esten vacios cuando el origen tenia un nombre.

#### 3.6 - observacion -> descripcion
En `gc_derecho` y `gc_fuenteadministrativa`, el campo `observacion` del CCA se mapea al campo `descripcion` de Django. Verifica igualdad de valores.

#### 3.7 - Valores por defecto
Verifica que todos los predios tengan:
- `interrelacionado` = `false`
- `nupre_fmi` = `false`
- `rectificacion_efecto_registral` = `false`

#### 3.8 - Localizacion por defecto (extdireccion)
Cuando la localizacion del origen es NULL, el ETL asigna un punto fijo por defecto: `ST_Point(4940023.7497, 2111479.6705, 9377)`. Verifica que ningun registro quede sin localizacion.

---

### Paso 5: Verificar integridad referencial (archivo 04)

Ejecutar `04_verificacion_integridad_referencial.sql`.

**Que hace:**
Detecta registros huerfanos en **24 relaciones de clave foranea** y verifica cobertura inversa.

#### Seccion A: Relaciones FK

Resultado:

| relacion | total_registros | fk_validas | fk_huerfanas | estado |
|----------|----------------|------------|-------------|--------|
| extdireccion.gc_predio_direccion -> gc_predio.id | 1500 | 1500 | 0 | OK |
| gc_derecho.baunit -> gc_predio.id | 2000 | 2000 | 0 | OK |

**Relaciones verificadas:**

| # | Tabla origen | Campo FK | Tabla destino |
|---|-------------|----------|---------------|
| 1 | extdireccion | gc_predio_direccion | gc_predio |
| 2 | dlc_datosadicionaleslevantamientocatastral | gc_predio | gc_predio |
| 3 | gc_estructuranovedadnumeropredial | gc_predio_novedad_numeros_prediales | dlc_datosadicionaleslevantamientocatastral |
| 4 | gc_unidadconstruccion | gc_construccion | gc_construccion |
| 5 | gc_unidadconstruccion | gc_caracteristicasunidadconstruccion | gc_caracteristicasunidadconstruccion |
| 6-11 | col_uebaunit | unidad, ue_gc_terreno, ue_gc_construccion, ue_gc_unidadconstruccion | gc_predio, gc_terreno, gc_construccion, gc_unidadconstruccion |
| 12 | cuc_calificacionconvencional | gc_caracteristicasunidadconstruccion | gc_caracteristicasunidadconstruccion |
| 13 | cuc_grupocalificacion | cuc_calificacion_convencional | cuc_calificacionconvencional |
| 14 | cuc_objetoconstruccion | cuc_grupo_calificacion | cuc_grupocalificacion |
| 15 | cuc_calificacionnoconvencional | gc_caracteristicasunidadconstruccion | gc_caracteristicasunidadconstruccion |
| 16-17 | col_miembros | interesado_gc_interesado, agrupacion | gc_interesado, gc_agrupacioninteresados |
| 18-20 | gc_derecho | baunit, interesado_gc_interesado, interesado_gc_agrupacioninteresados | gc_predio, gc_interesado, gc_agrupacioninteresados |
| 21-22 | col_rrrfuente | rrr_gc_derecho, fuente_administrativa | gc_derecho, gc_fuenteadministrativa |
| 23-24 | col_unidadfuente | unidad, fuente_administrativa | gc_predio, gc_fuenteadministrativa |

#### Seccion B: Cobertura inversa

Verifica que las entidades principales tengan las relaciones esperadas:

| cobertura | interpretacion |
|-----------|---------------|
| Predios sin terreno | Todo predio deberia tener al menos un terreno asociado |
| Predios sin derecho | Todo predio deberia tener al menos un derecho |
| Predios sin direccion | Todo predio deberia tener al menos una direccion |
| Predios sin datos adicionales | Todo predio deberia tener datos de levantamiento catastral |
| Construcciones sin unidad de construccion | Toda construccion deberia tener unidades |
| Derechos sin fuente administrativa | Todo derecho deberia tener una fuente |

El estado `REVISAR` no necesariamente indica un error; puede ser un caso valido del negocio (ej: un predio sin construcciones no tendra unidades de construccion).

---

### Paso 6: Muestreo campo a campo (archivo 05)

Ejecutar `05_verificacion_muestreo_campos.sql`.

**Que hace:**
Toma una muestra de **20 registros** por tabla y compara cada campo individualmente entre origen y destino usando dblink para hacer un JOIN cruzado.

**Resultado por cada campo:**

| id | campo | valor_origen | valor_destino | coincide |
|----|-------|-------------|---------------|----------|
| 101 | numero_predial | 25436000001 | 25436000001 | SI |
| 101 | departamento | 25 | 25 | SI |
| 101 | tiene_fmi | true | true | SI |

**Tablas y campos comparados:**

| Tabla | Campos comparados |
|-------|-------------------|
| gc_predio vs cca_predio | numero_predial, nupre, departamento, municipio, tiene_fmi, codigo_orip, matricula_inmobiliaria, nombre |
| gc_terreno vs cca_terreno | area, etiqueta, tiene_geometria |
| gc_interesado vs cca_interesado | documento_identidad, primer_nombre, primer_apellido, razon_social, nombre |
| gc_derecho vs cca_derecho | fraccion_derecho (con conversion /100), fecha_inicio_tenencia, descripcion vs observacion |
| gc_fuenteadministrativa vs cca_fuenteadministrativa | numero_fuente, fecha_documento_fuente, ente_emisor, descripcion vs observacion |
| gc_construccion vs cca_construccion | numero_pisos, area, anio_construccion, altura |
| dlc_datosadicionaleslevantamientocatastral vs cca_predio+usuario | observaciones, fecha_visita_predial, numero_documento_reconocedor |

Al final incluye un **resumen de discrepancias** con conteo total de coincidencias y no coincidencias por tabla.

---

## Interpretacion de Resultados

### Columna `estado`
- **`OK`**: La verificacion paso correctamente
- **`FALLO`**: Se detectaron problemas que requieren investigacion
- **`EXCESO`**: Hay mas registros de lo esperado (posibles duplicados)
- **`REVISAR`**: No es necesariamente un error, pero conviene revisar (cobertura inversa)
- **`N/A`**: No aplica (ej: no hay datos para comparar)
- **`INFO`**: Informacion adicional, no indica error

### Columna `coincide` (solo nivel 5)
- **`SI`**: El valor del campo coincide entre origen y destino
- **`NO`**: El valor difiere; revisar si es una transformacion esperada o un error

### Priorizacion de errores
1. **Nivel 1 (conteos)**: Si hay FALLOs aqui, hay perdida de datos. Investigar primero.
2. **Nivel 4 (integridad referencial)**: FKs huerfanas indican relaciones rotas. Segundo en prioridad.
3. **Nivel 2 (dominios)**: `nulls_inesperados > 0` indica que hay valores de dominio sin equivalencia.
4. **Nivel 3 (transformaciones)**: Verificar que las conversiones criticas se hicieron bien.
5. **Nivel 5 (muestreo)**: Revision visual de una muestra para detectar errores sutiles.

---

## Tablas Migradas (22 en total)

| # | Tabla destino (Django) | Tabla origen (CCA) | Tipo |
|---|----------------------|-------------------|------|
| 01 | gc_predio | cca_predio | Entidad principal |
| 02 | gc_terreno | cca_terreno | Unidad espacial |
| 03 | extdireccion | extdireccion (CCA) | Direcciones |
| 04 | col_uebaunit (terreno) | cca_terreno | Relacion predio-terreno |
| 05 | dlc_datosadicionaleslevantamientocatastral | cca_predio + cca_usuario | Datos levantamiento |
| 06 | gc_estructuranovedadnumeropredial | cca_estructuranovedadnumeropredial | Novedades |
| 07 | gc_construccion | cca_construccion | Unidad espacial |
| 08 | gc_caracteristicasunidadconstruccion | cca_caracteristicasunidadconstruccion | Caracteristicas |
| 09 | gc_unidadconstruccion | cca_unidadconstruccion | Unidad espacial |
| 10 | col_uebaunit (construccion) | cca_construccion | Relacion predio-construccion |
| 11 | col_uebaunit (unidad const.) | cca_unidadconstruccion | Relacion predio-unidad |
| 12 | cuc_calificacionconvencional | cca_calificacionconvencional | Calificacion |
| 13 | cuc_grupocalificacion | cca_calificacionconvencional (x5) | Grupos de calificacion |
| 14 | cuc_objetoconstruccion | cca_calificacionconvencional (x13) | Objetos construccion |
| 15 | cuc_calificacionnoconvencional | cca_caracteristicasunidadconstruccion | Calificacion no conv. |
| 16 | gc_interesado | cca_interesado | Partes interesadas |
| 17 | gc_agrupacioninteresados | cca_agrupacioninteresados | Agrupaciones |
| 18 | col_miembros | cca_miembros | Relacion miembros-agrupacion |
| 19 | gc_derecho | cca_derecho | Derechos |
| 20 | gc_fuenteadministrativa | cca_fuenteadministrativa | Fuentes |
| 21 | col_rrrfuente | cca_fuenteadministrativa_derecho | Relacion fuente-derecho |
| 22 | col_unidadfuente | cca_unidadfuente | Relacion fuente-predio |

---

## Notas Tecnicas

- Los archivos usan el esquema `cun25436` hardcoded (sin placeholder `{schema}`)
- La conexion dblink apunta a `host=localhost port=5433 dbname=ladm_col user=postgres password=carlos123`
- Cada archivo es independiente y puede ejecutarse por separado, siempre y cuando se haya ejecutado el archivo 00 primero
- Las condiciones WHERE en el nivel 1 replican las mismas condiciones que se usaron en los scripts de insert del ETL
- El nivel 5 usa `CROSS JOIN LATERAL VALUES` para pivotear multiples columnas en filas individuales comparables
- La normalizacion de calificaciones (1 fila CCA -> 5 grupos + hasta 13 objetos) se verifica con las formulas correspondientes en los niveles 1 y 2
