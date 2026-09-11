-- =============================================================================
-- Destino: gc_derechocatastral             Origen: tmp_derecho (gc_derecho de Django)
-- Reglas:
--   * local_id = id de Django; espacio_de_nombres = 'DJANGO.GC_DERECHO'.
--   * Se rechaza si el predio no se migro o si no hay interesado ni agrupacion migrados.
--   * fraccion_derecho y fecha_inicio_tenencia no existen en gc_derechocatastral:
--     se reportan como NO_MIGRADO cuando aportan informacion (fraccion distinta de 1).
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_derechocatastral', d.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_derecho AS d
LEFT JOIN {schema}.gc_predio AS p
       ON p.local_id = d.baunit AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
LEFT JOIN {schema}.gc_interesado AS i
       ON i.local_id = d.interesado_gc_interesado AND i.espacio_de_nombres = 'DJANGO.GC_INTERESADO'
LEFT JOIN {schema}.gc_agrupacioninteresados AS a
       ON a.local_id = d.interesado_gc_agrupacioninteresados AND a.espacio_de_nombres = 'DJANGO.GC_AGRUPACIONINTERESADOS'
CROSS JOIN LATERAL (VALUES
    ('unidad', d.baunit, 'RECHAZADO', 'Predio no migrado', p.t_id IS NULL),
    ('interesado', COALESCE(d.interesado_gc_interesado, d.interesado_gc_agrupacioninteresados), 'RECHAZADO',
     'Sin interesado ni agrupacion migrados', i.t_id IS NULL AND a.t_id IS NULL),
    ('tipo', d.tipo, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     d.tipo IS NOT NULL AND etl_control.dominio('{schema}', 'gc_derechocatastraltipo', d.tipo) IS NULL),
    ('fraccion_derecho', d.fraccion_derecho, 'NO_MIGRADO', 'gc_derechocatastral no tiene fraccion de derecho',
     d.fraccion_derecho IS NOT NULL AND d.fraccion_derecho::numeric <> 1),
    ('fecha_inicio_tenencia', d.fecha_inicio_tenencia, 'NO_MIGRADO', 'gc_derechocatastral no tiene fecha de inicio de tenencia',
     d.fecha_inicio_tenencia IS NOT NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT d.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_derechocatastral (
    t_basket,
    tipo,
    descripcion,
    interesado_gc_interesado,
    interesado_gc_agrupacioninteresados,
    unidad,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    etl_control.dominio('{schema}', 'gc_derechocatastraltipo', d.tipo),
    etl_control.texto(d.descripcion),
    i.t_id,
    a.t_id,
    p.t_id,
    COALESCE(d.comienzo_vida_util_version::timestamp, now()::timestamp),
    d.fin_vida_util_version::timestamp,
    'DJANGO.GC_DERECHO',
    d.id
FROM tmp_derecho AS d
JOIN {schema}.gc_predio AS p
  ON p.local_id = d.baunit AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
LEFT JOIN {schema}.gc_interesado AS i
       ON i.local_id = d.interesado_gc_interesado AND i.espacio_de_nombres = 'DJANGO.GC_INTERESADO'
LEFT JOIN {schema}.gc_agrupacioninteresados AS a
       ON a.local_id = d.interesado_gc_agrupacioninteresados AND a.espacio_de_nombres = 'DJANGO.GC_AGRUPACIONINTERESADOS'
WHERE NOT d.es_centinela
  AND (i.t_id IS NOT NULL OR a.t_id IS NOT NULL);
