-- =============================================================================
-- Destino: gc_predio_informalidad          Origen: tmp_predio_informalidad
-- Reglas (decision 2026-09-11: las mejoras pasan a Informal con su relacion):
--   * REGISTRADA  : relacion existente en Django.
--   * DERIVADA_NPN: predio formal = NPN del informal en posiciones 1-21 + '000000000'.
--   * area_terreno_interseccion = area del predio informal (para mejoras suele ser 0).
--   * area_construida_interseccion = area construida del predio informal.
--   * Informal sin predio formal identificable: no se crea relacion + RECHAZADO.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_predio_informalidad', r.id_informal, x.campo, x.valor, x.accion, x.motivo
FROM tmp_predio_informalidad AS r
LEFT JOIN {schema}.gc_predio AS pf ON pf.local_id = r.id_formal AND pf.espacio_de_nombres = 'DJANGO.GC_PREDIO'
LEFT JOIN {schema}.gc_predio AS pi ON pi.local_id = r.id_informal AND pi.espacio_de_nombres = 'DJANGO.GC_PREDIO'
CROSS JOIN LATERAL (VALUES
    ('cr_predio_formal', r.npn_informal, 'RECHAZADO',
     'Predio informal sin predio formal identificable por NPN (posiciones 1-21 + 000000000)',
     r.id_formal IS NULL),
    ('cr_predio_formal', r.id_formal, 'RECHAZADO', 'Predio formal o informal no migrado',
     r.id_formal IS NOT NULL AND (pf.t_id IS NULL OR pi.t_id IS NULL))
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT r.es_centinela AND x.aplica
UNION ALL
SELECT '{schema}', 'gc_predio_informalidad', NULL, 'cr_predio_formal', count(*)::text || ' relaciones', 'DERIVADO',
       'Relacion informal-formal derivada por NPN (no estaba registrada en Django)'
FROM tmp_predio_informalidad AS r
WHERE NOT r.es_centinela AND r.origen_relacion = 'DERIVADA_NPN' AND r.id_formal IS NOT NULL
HAVING count(*) > 0;

INSERT INTO {schema}.gc_predio_informalidad (
    t_basket,
    cr_predio_formal,
    cr_predio_informal,
    area_terreno_interseccion,
    area_construida_interseccion
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    pf.t_id,
    pi.t_id,
    r.area_informal::numeric,
    r.area_construida_informal::numeric
FROM tmp_predio_informalidad AS r
JOIN {schema}.gc_predio AS pf ON pf.local_id = r.id_formal AND pf.espacio_de_nombres = 'DJANGO.GC_PREDIO'
JOIN {schema}.gc_predio AS pi ON pi.local_id = r.id_informal AND pi.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT r.es_centinela;
