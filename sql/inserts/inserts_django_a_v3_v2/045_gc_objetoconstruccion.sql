-- =============================================================================
-- Destino: gc_objetoconstruccion           Origen: tmp_objeto_construccion
-- Requiere map_grupo (044). puntos: v3 admite 0..34 (Django 0..50); fuera de rango -> NULL.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_objetoconstruccion', o.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_objeto_construccion AS o
LEFT JOIN map_grupo AS mg ON mg.id_origen = o.id_grupo::bigint
CROSS JOIN LATERAL (VALUES
    ('gc_grupo_calificacion', o.id_grupo, 'RECHAZADO', 'Grupo de calificacion no migrado',
     mg.t_id IS NULL),
    ('puntos', o.puntos, 'NULIFICADO', 'Fuera del rango 0..34 de v3',
     o.puntos IS NOT NULL AND NOT (o.puntos::integer BETWEEN 0 AND 34)),
    ('tipo_objeto_construccion', o.tipo_objeto_construccion, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     o.tipo_objeto_construccion IS NOT NULL
     AND etl_control.dominio('{schema}', 'gc_objetoconstrucciontipo', o.tipo_objeto_construccion) IS NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT o.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_objetoconstruccion (
    t_basket,
    tipo_objeto_construccion,
    puntos,
    gc_grupo_calificacion
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    etl_control.dominio('{schema}', 'gc_objetoconstrucciontipo', o.tipo_objeto_construccion),
    CASE WHEN o.puntos::integer BETWEEN 0 AND 34 THEN o.puntos::integer END,
    mg.t_id
FROM tmp_objeto_construccion AS o
JOIN map_grupo AS mg ON mg.id_origen = o.id_grupo::bigint
WHERE NOT o.es_centinela;
