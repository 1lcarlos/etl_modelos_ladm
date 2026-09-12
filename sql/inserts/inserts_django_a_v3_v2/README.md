# Flujo Django (modelo interno) → modelo interno v3 (ACC)

Queries en `sql/queries/queries_django_a_v3_v2/`, inserts en esta carpeta y orden en
`sql/insert_order_django_a_v3_v2.txt`. El motor Python no cambia: todo se resuelve en SQL.

## Configuración (GUI / `etl_config.json`)

| Campo | Valor |
|---|---|
| `paths.queries` | `.../sql/queries/queries_django_a_v3_v2` |
| `paths.inserts` | `.../sql/inserts/inserts_django_a_v3_v2` |
| `paths.order_file` | `.../sql/insert_order_django_a_v3_v2.txt` |
| Origen | BD Django (debe tener `public.cadaster_information` y `public.tmp_tramite`) |
| Destino | BD v3 creada con ili2pg (BasketHandling=readWrite) |

Las dos tablas de `public` viven fuera del esquema del municipio: `cadaster_information` da la
vigencia de actualización catastral (`predio.sql`) y `tmp_tramite` los trámites de todos los
municipios (`tramite_catastral.sql`, que filtra por el código del esquema).

## Convenciones

1. **Fila centinela.** Cada query devuelve primero una fila con `es_centinela = true` y el resto de
   columnas en NULL. Con esto el motor crea la tabla temporal con todas sus columnas aunque el esquema
   no tenga datos, y todas las columnas quedan tipo `TEXT` (no depende del tipo de la primera fila).
   Todo insert filtra `WHERE NOT es_centinela` y hace cast explícito (`::bigint`, `::date`, `::geometry`...).
2. **Identificadores.** `t_id` sale de `t_ili2db_seq`; `local_id` = id de Django;
   `espacio_de_nombres` = `DJANGO.<TABLA>`. Las llaves foráneas se resuelven por
   `local_id + espacio_de_nombres` en el destino, nunca por el `local_id` de Django (que contiene NPN,
   ids heredados o está vacío).
   Excepción: `gc_caracteristicasunidadconstruccion` y las calificaciones no tienen `local_id` en v3;
   se enlazan con las tablas temporales `map_caracteristica`, `map_calificacion` y `map_grupo`.
3. **Baskets.** `000` crea el dataset `Baseline` y dos baskets: `Modelo_Interno_ACC_V3.Modelo_Interno`
   (gc_*, col_*, ext*) y `Submodelo_Cartografia_Catastral_V1_1.LimitesOperativosCatastro` (cc_*).
4. **Dominios.** `etl_control.dominio(esquema, tabla, valor)` busca primero en
   `etl_control.mapeo_dominio` (`001_mapeo_dominios.sql`) y luego coincidencia exacta de `ilicode`.
   No hay `ILIKE` ni "primer valor de la tabla" como respaldo.
5. **Incidencias.** Cada insert registra en `etl_control.incidencia` lo que no se pudo migrar tal cual:
   `RECHAZADO` (fila no cargada), `NULIFICADO` (campo en NULL), `DERIVADO` (valor calculado por regla),
   `NO_MIGRADO` (atributo sin columna en v3), `ADVERTENCIA`. La sentencia principal de carga es siempre
   la última del archivo, para que el conteo del log corresponda a filas cargadas.
6. **Tablas de control entre comillas.** Se escriben como `"etl_control"."incidencia"` para que el motor
   no las trunque al iniciar cada esquema.
7. **Sin datos inventados.** Si Django no tiene el dato, queda NULL (decisión 2026-09-11). Para eso
   `002_relajar_obligatorios.sql` quita el NOT NULL de los atributos obligatorios afectados; la lista
   de faltantes sale en `manual/reporte_migracion.sql` y `manual/restaurar_obligatorios.sql` los vuelve
   a exigir. Únicas excepciones: `comienzo_vida_util_version` (fecha de carga si falta), la
   `fecha_inscripcion` de los trámites (decisión 2026-09-11: la fecha de la resolución) y los valores de
   "desconocido" que trae el propio dominio (`Sin_Informacion`, `Sin_Determinar`, `No_Aplica`, `Desconocido`).

## Reglas de negocio principales

| Tema | Regla |
|---|---|
| Matrícula | Numérica entre 1 y 9.999.999.999; si no, NULL. Ya no se usa 1 como relleno ni se descartan las de 6+ dígitos. |
| ORIP / NUPRE | Se copian tal cual; `codigo_homologado` = NUPRE. Sin rellenos (`0000`, `BBK00000`). |
| Tipo de predio | Sin dato → `Predio.Privado`. Ejido, territorio colectivo y vacante → NULL (pendiente). |
| Mejoras | Condición `Informal` + relación en `gc_predio_informalidad` con el predio formal del mismo NPN (posiciones 1-21 + `000000000`). |
| Unidades de construcción | Todas las que tienen geometría. Enlace al predio: directo → vía construcción → NPN del `local_id`. |
| Avalúos | Histórico completo; se excluyen los marcados `BORRAR`. |
| Derechos | `gc_derechocatastral` (Dominio / Posesión / Ocupación). |
| Direcciones | Sin localización → NULL (sin coordenadas fijas). |
| Puntos lindero | `desacuerdo` solo si Django lo indica; sin dato → NULL. |
| Trámites | Un `gc_tramitecatastral` por resolución de `public.tmp_tramite` (filtrada por municipio del esquema). `fecha_inscripcion` = fecha de la resolución (`DERIVADO`); `numero_radicado` no existe en v3 (`NO_MIGRADO`); tipo sin equivalente → fila rechazada. |
| Predio ↔ trámite | `gc_predio_tramitecatastral` enlaza por `local_id` del predio y `numero_resolucion`. No condiciona la migración de predios: los predios sin trámite se migran igual. |
| cc_nomenclaturavial | Omitida: la geometría cambió en v3. |

## Pendientes

- Mapeos sin equivalente (ver `SELECT * FROM etl_control.mapeo_dominio WHERE ilicode_destino IS NULL`).
- Confirmar: `col_areavalor` como `Area_Alfanumerica`; dueño de `gc_estructuraprediomatriznpn` (unidad);
  `puntotipo` de los puntos de control.
- Trámites: `Cancelación de predios` se mapea a `Cancelaciones.Fuerza_Mayor` y
  `Procedimientos catastrales con efectos registrales` a `Rectificaciones.Errores_Inscripcion_Catastral`
  (decisiones 2026-09-11); el origen no distingue más detalle.
- Fuera de alcance: responsables de fuente, restricciones, ofertas, zonas homogéneas.

## Verificación

1. `manual/reporte_migracion.sql`: conciliación, incidencias, mapeos pendientes y deuda de datos.
2. Exportar con ili2pg (`--export --dataset Baseline`) y validar el XTF con ilivalidator.
