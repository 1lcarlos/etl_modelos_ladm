-- =============================================================================
-- Destino: gc_terreno                      Origen: tmp_terreno (terreno.sql)
-- Reglas:
--   * local_id = id de Django; espacio_de_nombres = 'DJANGO.GC_TERRENO'.
--   * Sin geometria: se rechaza (la geometria es obligatoria y es la razon de ser del terreno).
--   * Geometria invalida: se migra sin corregir y se registra ADVERTENCIA.
--   * El area se migra en 048_col_areavalor; observacion no tiene columna en v3.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_terreno', t.id, 'geometria', NULL, 'RECHAZADO', 'Terreno sin geometria'
FROM tmp_terreno AS t
WHERE NOT t.es_centinela AND t.geometria IS NULL
UNION ALL
SELECT '{schema}', 'gc_terreno', t.id, 'geometria', ST_IsValidReason(t.geometria::geometry), 'ADVERTENCIA',
       'Geometria invalida (se migra sin corregir)'
FROM tmp_terreno AS t
WHERE NOT t.es_centinela AND t.geometria IS NOT NULL AND NOT ST_IsValid(t.geometria::geometry)
UNION ALL
SELECT '{schema}', 'gc_terreno', t.id, 'observacion', left(t.observacion, 255), 'NO_MIGRADO',
       'El modelo destino no tiene este atributo'
FROM tmp_terreno AS t
WHERE NOT t.es_centinela AND etl_control.texto(t.observacion) IS NOT NULL;

INSERT INTO {schema}.gc_terreno (
    t_basket,
    geometria,
    codigo,
    etiqueta,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    t.geometria::geometry,
    etl_control.texto(t.codigo),
    etl_control.texto(t.etiqueta),
    COALESCE(t.comienzo_vida_util_version::timestamp, now()::timestamp),
    t.fin_vida_util_version::timestamp,
    'DJANGO.GC_TERRENO',
    t.id
FROM tmp_terreno AS t
WHERE NOT t.es_centinela
  AND t.geometria IS NOT NULL;
