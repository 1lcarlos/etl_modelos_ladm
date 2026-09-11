-- =============================================================================
-- Destino: gc_caracteristicasunidadconstruccion
-- Origen : tmp_caracteristicas_unidadconstruccion
-- En v3 esta tabla no tiene local_id. Para enlazar unidades y calificaciones se crea la
-- tabla temporal map_caracteristica (id de Django -> t_id destino), que usan 041, 043,
-- 046 y 047 en la misma sesion.
-- Reglas (decision 2026-09-11):
--   * tipo_unidad_construccion, uso, anio_construccion, total_plantas y estado_conservacion
--     quedan NULL cuando Django no los tiene (antes se forzaban Anexo/Kioscos/1512/Bueno).
--   * tipo_construccion y puntaje no tienen columna en v3.
-- =============================================================================

DROP TABLE IF EXISTS map_caracteristica;
CREATE TEMP TABLE map_caracteristica AS
SELECT c.id::bigint AS id_origen,
       nextval('{schema}.t_ili2db_seq'::regclass) AS t_id
FROM tmp_caracteristicas_unidadconstruccion AS c
WHERE NOT c.es_centinela;

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_caracteristicasunidadconstruccion', c.id, x.campo, x.valor, 'NULIFICADO',
       'Valor sin equivalente en el dominio destino'
FROM tmp_caracteristicas_unidadconstruccion AS c
CROSS JOIN LATERAL (VALUES
    ('tipo_unidad_construccion', c.tipo_unidad_construccion,
     c.tipo_unidad_construccion IS NOT NULL
     AND etl_control.dominio('{schema}', 'gc_unidadconstrucciontipo', c.tipo_unidad_construccion) IS NULL),
    ('uso', c.uso,
     c.uso IS NOT NULL AND etl_control.dominio('{schema}', 'gc_usouconstipo', c.uso) IS NULL)
) AS x(campo, valor, aplica)
WHERE NOT c.es_centinela AND x.aplica
UNION ALL
SELECT '{schema}', 'gc_caracteristicasunidadconstruccion', NULL, 'tipo_construccion',
       count(*)::text || ' caracteristicas con valor', 'NO_MIGRADO',
       'v3 no tiene tipo de construccion (convencional / no convencional) en la caracteristica'
FROM tmp_caracteristicas_unidadconstruccion AS c
WHERE NOT c.es_centinela AND c.tipo_construccion IS NOT NULL
HAVING count(*) > 0
UNION ALL
SELECT '{schema}', 'gc_caracteristicasunidadconstruccion', NULL, 'puntaje',
       count(*)::text || ' caracteristicas con valor', 'NO_MIGRADO',
       'v3 no tiene puntaje en la caracteristica'
FROM tmp_caracteristicas_unidadconstruccion AS c
WHERE NOT c.es_centinela AND c.puntaje IS NOT NULL
HAVING count(*) > 0;

INSERT INTO {schema}.gc_caracteristicasunidadconstruccion (
    t_id,
    t_basket,
    identificador,
    tipo_unidad_construccion,
    total_plantas,
    uso,
    anio_construccion,
    area_construida,
    estado_conservacion,
    observaciones,
    total_habitaciones,
    total_banios,
    total_locales
)
SELECT
    m.t_id,
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    COALESCE(etl_control.texto(c.identificador), c.identificador),
    etl_control.dominio('{schema}', 'gc_unidadconstrucciontipo', c.tipo_unidad_construccion),
    c.total_plantas::integer,
    etl_control.dominio('{schema}', 'gc_usouconstipo', c.uso),
    c.anio_construccion::integer,
    c.area::numeric,
    NULL,
    etl_control.texto(c.observacion),
    c.total_habitaciones::integer,
    c.total_banios::integer,
    c.total_locales::integer
FROM tmp_caracteristicas_unidadconstruccion AS c
JOIN map_caracteristica AS m ON m.id_origen = c.id::bigint
WHERE NOT c.es_centinela;
