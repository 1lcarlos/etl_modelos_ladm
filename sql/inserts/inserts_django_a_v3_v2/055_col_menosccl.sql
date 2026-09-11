-- =============================================================================
-- Destino: col_menosccl                    Origen: tmp_col_menosccl
-- Misma regla de col_masccl.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'col_menosccl', r.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_col_menosccl AS r
LEFT JOIN {schema}.gc_lindero AS l
       ON l.local_id = r.ccl AND l.espacio_de_nombres = 'DJANGO.GC_LINDERO'
LEFT JOIN {schema}.gc_terreno AS t
       ON t.local_id = r.ue_terreno AND t.espacio_de_nombres = 'DJANGO.GC_TERRENO'
LEFT JOIN {schema}.gc_unidadconstruccion AS u
       ON u.local_id = r.ue_unidadconstruccion AND u.espacio_de_nombres = 'DJANGO.GC_UNIDADCONSTRUCCION'
CROSS JOIN LATERAL (VALUES
    ('ue', COALESCE(r.ue_construccion, r.ue_servidumbre), 'NO_MIGRADO',
     'La UE es construccion o servidumbre (no existen en v3)',
     r.ue_terreno IS NULL AND r.ue_unidadconstruccion IS NULL),
    ('ccl/ue', r.ccl, 'RECHAZADO', 'Lindero o UE no migrado',
     (r.ue_terreno IS NOT NULL OR r.ue_unidadconstruccion IS NOT NULL)
     AND (l.t_id IS NULL OR (t.t_id IS NULL AND u.t_id IS NULL)))
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT r.es_centinela AND x.aplica;

INSERT INTO {schema}.col_menosccl (t_basket, ccl_menos, ue_menos_gc_unidadconstruccion, ue_menos_gc_terreno)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    l.t_id,
    u.t_id,
    t.t_id
FROM tmp_col_menosccl AS r
JOIN {schema}.gc_lindero AS l
  ON l.local_id = r.ccl AND l.espacio_de_nombres = 'DJANGO.GC_LINDERO'
LEFT JOIN {schema}.gc_terreno AS t
       ON t.local_id = r.ue_terreno AND t.espacio_de_nombres = 'DJANGO.GC_TERRENO'
LEFT JOIN {schema}.gc_unidadconstruccion AS u
       ON u.local_id = r.ue_unidadconstruccion AND u.espacio_de_nombres = 'DJANGO.GC_UNIDADCONSTRUCCION'
WHERE NOT r.es_centinela
  AND (t.t_id IS NOT NULL OR u.t_id IS NOT NULL);
