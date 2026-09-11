-- =============================================================================
-- Destino: col_cclfuente (lindero -> fuente espacial)   Origen: tmp_col_cclfuente
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'col_cclfuente', r.id, 'ccl/fuente_espacial', r.ccl || ' / ' || r.fuente_espacial,
       'RECHAZADO', 'Lindero o fuente espacial no migrado'
FROM tmp_col_cclfuente AS r
LEFT JOIN {schema}.gc_lindero AS l
       ON l.local_id = r.ccl AND l.espacio_de_nombres = 'DJANGO.GC_LINDERO'
LEFT JOIN {schema}.gc_fuenteespacial AS f
       ON f.local_id = r.fuente_espacial AND f.espacio_de_nombres = 'DJANGO.GC_FUENTEESPACIAL'
WHERE NOT r.es_centinela AND (l.t_id IS NULL OR f.t_id IS NULL);

INSERT INTO {schema}.col_cclfuente (t_basket, ccl, fuente_espacial)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    l.t_id,
    f.t_id
FROM tmp_col_cclfuente AS r
JOIN {schema}.gc_lindero AS l
  ON l.local_id = r.ccl AND l.espacio_de_nombres = 'DJANGO.GC_LINDERO'
JOIN {schema}.gc_fuenteespacial AS f
  ON f.local_id = r.fuente_espacial AND f.espacio_de_nombres = 'DJANGO.GC_FUENTEESPACIAL'
WHERE NOT r.es_centinela;
