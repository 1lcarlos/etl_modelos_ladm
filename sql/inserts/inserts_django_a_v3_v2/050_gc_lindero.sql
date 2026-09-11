-- =============================================================================
-- Destino: gc_lindero                      Origen: tmp_lindero
-- Reglas:
--   * local_id = id de Django; espacio_de_nombres = 'DJANGO.GC_LINDERO'.
--   * Sin geometria: se rechaza.
--   * longitud sin dato: se calcula de la geometria (ST_Length) + incidencia DERIVADO.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_lindero', l.id, 'geometria', NULL, 'RECHAZADO', 'Lindero sin geometria'
FROM tmp_lindero AS l
WHERE NOT l.es_centinela AND l.geometria IS NULL
UNION ALL
SELECT '{schema}', 'gc_lindero', l.id, 'longitud', round(ST_Length(l.geometria::geometry)::numeric, 1)::text,
       'DERIVADO', 'Sin longitud en origen: se calcula de la geometria'
FROM tmp_lindero AS l
WHERE NOT l.es_centinela AND l.geometria IS NOT NULL AND l.longitud IS NULL;

INSERT INTO {schema}.gc_lindero (
    t_basket,
    longitud,
    geometria,
    localizacion_textual,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    COALESCE(l.longitud::numeric, round(ST_Length(l.geometria::geometry)::numeric, 1)),
    l.geometria::geometry,
    etl_control.texto(l.localizacion_textual),
    COALESCE(l.comienzo_vida_util_version::timestamp, now()::timestamp),
    l.fin_vida_util_version::timestamp,
    'DJANGO.GC_LINDERO',
    l.id
FROM tmp_lindero AS l
WHERE NOT l.es_centinela
  AND l.geometria IS NOT NULL;
