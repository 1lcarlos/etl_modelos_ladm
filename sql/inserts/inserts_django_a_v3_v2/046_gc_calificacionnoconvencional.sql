-- =============================================================================
-- Destino: gc_calificacionnoconvencional   Origen: tmp_calificacion_no_convencional
-- Requiere map_caracteristica (040).
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_calificacionnoconvencional', c.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_calificacion_no_convencional AS c
LEFT JOIN map_caracteristica AS m ON m.id_origen = c.id_caracteristica::bigint
CROSS JOIN LATERAL (VALUES
    ('gc_caracteristica_uconstruccion', c.id_caracteristica, 'RECHAZADO', 'Caracteristica no migrada',
     m.t_id IS NULL),
    ('tipo_anexo', c.tipo_anexo, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     c.tipo_anexo IS NOT NULL AND etl_control.dominio('{schema}', 'gc_anexotipo', c.tipo_anexo) IS NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT c.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_calificacionnoconvencional (
    t_basket,
    tipo_anexo,
    gc_caracteristica_uconstruccion
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    etl_control.dominio('{schema}', 'gc_anexotipo', c.tipo_anexo),
    m.t_id
FROM tmp_calificacion_no_convencional AS c
JOIN map_caracteristica AS m ON m.id_origen = c.id_caracteristica::bigint
WHERE NOT c.es_centinela;
