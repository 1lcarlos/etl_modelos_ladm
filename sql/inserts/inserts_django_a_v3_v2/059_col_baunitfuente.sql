-- =============================================================================
-- Destino: col_baunitfuente (predio -> fuente espacial)   Origen: tmp_col_baunitfuente
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'col_baunitfuente', r.id, 'unidad/fuente_espacial', r.unidad || ' / ' || r.fuente,
       'RECHAZADO', 'Predio o fuente espacial no migrado'
FROM tmp_col_baunitfuente AS r
LEFT JOIN {schema}.gc_predio AS p
       ON p.local_id = r.unidad AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
LEFT JOIN {schema}.gc_fuenteespacial AS f
       ON f.local_id = r.fuente AND f.espacio_de_nombres = 'DJANGO.GC_FUENTEESPACIAL'
WHERE NOT r.es_centinela AND (p.t_id IS NULL OR f.t_id IS NULL);

INSERT INTO {schema}.col_baunitfuente (t_basket, fuente_espacial, unidad)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    f.t_id,
    p.t_id
FROM tmp_col_baunitfuente AS r
JOIN {schema}.gc_predio AS p
  ON p.local_id = r.unidad AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
JOIN {schema}.gc_fuenteespacial AS f
  ON f.local_id = r.fuente AND f.espacio_de_nombres = 'DJANGO.GC_FUENTEESPACIAL'
WHERE NOT r.es_centinela;
