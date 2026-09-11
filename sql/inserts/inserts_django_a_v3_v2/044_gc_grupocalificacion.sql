-- =============================================================================
-- Destino: gc_grupocalificacion            Origen: tmp_grupo_calificacion
-- Crea map_grupo (id de Django -> t_id) para 045_gc_objetoconstruccion.
-- Requiere map_calificacion (043).
-- =============================================================================

DROP TABLE IF EXISTS map_grupo;
CREATE TEMP TABLE map_grupo AS
SELECT g.id::bigint AS id_origen,
       nextval('{schema}.t_ili2db_seq'::regclass) AS t_id,
       mc.t_id AS t_id_calificacion
FROM tmp_grupo_calificacion AS g
JOIN map_calificacion AS mc ON mc.id_origen = g.id_calificacion::bigint
WHERE NOT g.es_centinela;

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_grupocalificacion', g.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_grupo_calificacion AS g
LEFT JOIN map_grupo AS mg ON mg.id_origen = g.id::bigint
CROSS JOIN LATERAL (VALUES
    ('gc_calificacion_convencional', g.id_calificacion, 'RECHAZADO', 'Calificacion convencional no migrada',
     mg.t_id IS NULL),
    ('clase_calificacion', g.clase_calificacion, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     g.clase_calificacion IS NOT NULL AND etl_control.dominio('{schema}', 'gc_clasecalificaciontipo', g.clase_calificacion) IS NULL),
    ('conservacion', g.conservacion, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     g.conservacion IS NOT NULL AND etl_control.dominio('{schema}', 'gc_estadoconservaciontipo', g.conservacion) IS NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT g.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_grupocalificacion (
    t_id,
    t_basket,
    clase_calificacion,
    conservacion,
    subtotal,
    gc_calificacion_convencional
)
SELECT
    mg.t_id,
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    etl_control.dominio('{schema}', 'gc_clasecalificaciontipo', g.clase_calificacion),
    etl_control.dominio('{schema}', 'gc_estadoconservaciontipo', g.conservacion),
    g.subtotal::integer,
    mg.t_id_calificacion
FROM tmp_grupo_calificacion AS g
JOIN map_grupo AS mg ON mg.id_origen = g.id::bigint
WHERE NOT g.es_centinela;
