-- =============================================================================
-- Destino: gc_puntolindero                 Origen: tmp_punto_lindero
-- Reglas:
--   * local_id = id de Django; espacio_de_nombres = 'DJANGO.GC_PUNTOLINDERO'.
--   * puntotipo: Poste -> Catastro.Poste, etc. (001_mapeo_dominios.sql).
--   * desacuerdo: Acuerdo -> false, Desacuerdo -> true, sin dato -> NULL
--     (antes se marcaba true: todos los puntos quedaban en disputa).
--   * exactitud_vertical no existe en v3.
--   * Sin geometria: se rechaza.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_puntolindero', pl.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_punto_lindero AS pl
CROSS JOIN LATERAL (VALUES
    ('geometria', NULL, 'RECHAZADO', 'Punto sin geometria', pl.geometria IS NULL),
    ('puntotipo', pl.punto_tipo, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     pl.punto_tipo IS NOT NULL AND etl_control.dominio('{schema}', 'col_puntotipo', pl.punto_tipo) IS NULL),
    ('metodoproduccion', pl.metodo_produccion, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     pl.metodo_produccion IS NOT NULL AND etl_control.dominio('{schema}', 'col_metodoproducciontipo', pl.metodo_produccion) IS NULL),
    ('fotoidentificacion', pl.fotoidentificacion, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     pl.fotoidentificacion IS NOT NULL AND etl_control.dominio('{schema}', 'gc_fotoidentificaciontipo', pl.fotoidentificacion) IS NULL),
    ('exactitud_vertical', pl.exactitud_vertical, 'NO_MIGRADO', 'El modelo destino no tiene este atributo',
     pl.exactitud_vertical IS NOT NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT pl.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_puntolindero (
    t_basket,
    id_punto_lindero,
    puntotipo,
    desacuerdo,
    fotoidentificacion,
    exactitud_horizontal,
    metodoproduccion,
    geometria,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    pl.id_punto,
    etl_control.dominio('{schema}', 'col_puntotipo', pl.punto_tipo),
    CASE pl.acuerdo WHEN 'Acuerdo' THEN false WHEN 'Desacuerdo' THEN true END,
    etl_control.dominio('{schema}', 'gc_fotoidentificaciontipo', pl.fotoidentificacion),
    pl.exactitud_horizontal::numeric,
    etl_control.dominio('{schema}', 'col_metodoproducciontipo', pl.metodo_produccion),
    pl.geometria::geometry,
    COALESCE(pl.comienzo_vida_util_version::timestamp, now()::timestamp),
    pl.fin_vida_util_version::timestamp,
    'DJANGO.GC_PUNTOLINDERO',
    pl.id
FROM tmp_punto_lindero AS pl
WHERE NOT pl.es_centinela
  AND pl.geometria IS NOT NULL;
