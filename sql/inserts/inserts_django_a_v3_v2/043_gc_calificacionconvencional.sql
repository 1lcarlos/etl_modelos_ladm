-- =============================================================================
-- Destino: gc_calificacionconvencional     Origen: tmp_calificacion_convencional
-- Crea map_calificacion (id de Django -> t_id) para 044_gc_grupocalificacion.
-- Requiere map_caracteristica (040).
-- =============================================================================

DROP TABLE IF EXISTS map_calificacion;
CREATE TEMP TABLE map_calificacion AS
SELECT c.id::bigint AS id_origen,
       nextval('{schema}.t_ili2db_seq'::regclass) AS t_id,
       m.t_id AS t_id_caracteristica
FROM tmp_calificacion_convencional AS c
JOIN map_caracteristica AS m ON m.id_origen = c.id_caracteristica::bigint
WHERE NOT c.es_centinela;

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_calificacionconvencional', c.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_calificacion_convencional AS c
LEFT JOIN map_calificacion AS mc ON mc.id_origen = c.id::bigint
CROSS JOIN LATERAL (VALUES
    ('gc_caracteristica_uconstruccion', c.id_caracteristica, 'RECHAZADO', 'Caracteristica no migrada',
     mc.t_id IS NULL),
    ('tipo_calificar', c.tipo_calificar, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     c.tipo_calificar IS NOT NULL AND etl_control.dominio('{schema}', 'gc_calificartipo', c.tipo_calificar) IS NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT c.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_calificacionconvencional (
    t_id,
    t_basket,
    tipo_calificar,
    total_calificacion,
    gc_caracteristica_uconstruccion
)
SELECT
    mc.t_id,
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    etl_control.dominio('{schema}', 'gc_calificartipo', c.tipo_calificar),
    c.total_calificacion::integer,
    mc.t_id_caracteristica
FROM tmp_calificacion_convencional AS c
JOIN map_calificacion AS mc ON mc.id_origen = c.id::bigint
WHERE NOT c.es_centinela;
