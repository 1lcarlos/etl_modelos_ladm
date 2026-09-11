-- =============================================================================
-- Destino: col_uefuente (terreno o unidad -> fuente espacial)   Origen: tmp_col_uefuente
-- Construccion y servidumbre no existen en v3.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'col_uefuente', r.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_col_uefuente AS r
LEFT JOIN {schema}.gc_fuenteespacial AS f
       ON f.local_id = r.fuente_espacial AND f.espacio_de_nombres = 'DJANGO.GC_FUENTEESPACIAL'
LEFT JOIN {schema}.gc_terreno AS t
       ON t.local_id = r.ue_terreno AND t.espacio_de_nombres = 'DJANGO.GC_TERRENO'
LEFT JOIN {schema}.gc_unidadconstruccion AS u
       ON u.local_id = r.ue_unidadconstruccion AND u.espacio_de_nombres = 'DJANGO.GC_UNIDADCONSTRUCCION'
CROSS JOIN LATERAL (VALUES
    ('ue', COALESCE(r.ue_construccion, r.ue_servidumbre), 'NO_MIGRADO',
     'La UE es construccion o servidumbre (no existen en v3)',
     r.ue_terreno IS NULL AND r.ue_unidadconstruccion IS NULL),
    ('fuente/ue', r.fuente_espacial, 'RECHAZADO', 'Fuente espacial o UE no migrada',
     (r.ue_terreno IS NOT NULL OR r.ue_unidadconstruccion IS NOT NULL)
     AND (f.t_id IS NULL OR (t.t_id IS NULL AND u.t_id IS NULL)))
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT r.es_centinela AND x.aplica;

INSERT INTO {schema}.col_uefuente (t_basket, ue_gc_unidadconstruccion, ue_gc_terreno, fuente_espacial)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    u.t_id,
    t.t_id,
    f.t_id
FROM tmp_col_uefuente AS r
JOIN {schema}.gc_fuenteespacial AS f
  ON f.local_id = r.fuente_espacial AND f.espacio_de_nombres = 'DJANGO.GC_FUENTEESPACIAL'
LEFT JOIN {schema}.gc_terreno AS t
       ON t.local_id = r.ue_terreno AND t.espacio_de_nombres = 'DJANGO.GC_TERRENO'
LEFT JOIN {schema}.gc_unidadconstruccion AS u
       ON u.local_id = r.ue_unidadconstruccion AND u.espacio_de_nombres = 'DJANGO.GC_UNIDADCONSTRUCCION'
WHERE NOT r.es_centinela
  AND (t.t_id IS NOT NULL OR u.t_id IS NOT NULL);
