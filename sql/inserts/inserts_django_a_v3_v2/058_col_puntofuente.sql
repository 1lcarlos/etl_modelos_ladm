-- =============================================================================
-- Destino: col_puntofuente (punto -> fuente espacial)   Origen: tmp_col_puntofuente
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'col_puntofuente', r.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_col_puntofuente AS r
LEFT JOIN {schema}.gc_fuenteespacial AS f
       ON f.local_id = r.fuente_espacial AND f.espacio_de_nombres = 'DJANGO.GC_FUENTEESPACIAL'
LEFT JOIN {schema}.gc_puntolindero AS pl
       ON pl.local_id = r.punto_gc_puntolindero AND pl.espacio_de_nombres = 'DJANGO.GC_PUNTOLINDERO'
LEFT JOIN {schema}.gc_puntocontrol AS pc
       ON pc.local_id = r.punto_gc_puntocontrol AND pc.espacio_de_nombres = 'DJANGO.GC_PUNTOCONTROL'
CROSS JOIN LATERAL (VALUES
    ('punto_gc_puntolevantamiento', r.punto_gc_puntolevantamiento, 'NO_MIGRADO',
     'v3 no tiene puntos de levantamiento', r.punto_gc_puntolevantamiento IS NOT NULL),
    ('fuente/punto', r.fuente_espacial, 'RECHAZADO', 'Fuente espacial o punto no migrado',
     (r.punto_gc_puntolindero IS NOT NULL OR r.punto_gc_puntocontrol IS NOT NULL)
     AND (f.t_id IS NULL OR (pl.t_id IS NULL AND pc.t_id IS NULL)))
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT r.es_centinela AND x.aplica;

INSERT INTO {schema}.col_puntofuente (t_basket, fuente_espacial, punto_gc_puntolindero, punto_gc_puntocontrol)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    f.t_id,
    pl.t_id,
    pc.t_id
FROM tmp_col_puntofuente AS r
JOIN {schema}.gc_fuenteespacial AS f
  ON f.local_id = r.fuente_espacial AND f.espacio_de_nombres = 'DJANGO.GC_FUENTEESPACIAL'
LEFT JOIN {schema}.gc_puntolindero AS pl
       ON pl.local_id = r.punto_gc_puntolindero AND pl.espacio_de_nombres = 'DJANGO.GC_PUNTOLINDERO'
LEFT JOIN {schema}.gc_puntocontrol AS pc
       ON pc.local_id = r.punto_gc_puntocontrol AND pc.espacio_de_nombres = 'DJANGO.GC_PUNTOCONTROL'
WHERE NOT r.es_centinela
  AND (pl.t_id IS NOT NULL OR pc.t_id IS NOT NULL);
