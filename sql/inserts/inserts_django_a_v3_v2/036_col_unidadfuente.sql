-- =============================================================================
-- Destino: col_unidadfuente (fuente administrativa -> predio)
-- Origen : tmp_col_unidadfuente
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'col_unidadfuente', u.id, 'fuente/unidad',
       u.fuente_administrativa || ' / ' || u.unidad, 'RECHAZADO', 'Fuente administrativa o predio no migrado'
FROM tmp_col_unidadfuente AS u
LEFT JOIN {schema}.gc_fuenteadministrativa AS f
       ON f.local_id = u.fuente_administrativa AND f.espacio_de_nombres = 'DJANGO.GC_FUENTEADMINISTRATIVA'
LEFT JOIN {schema}.gc_predio AS p
       ON p.local_id = u.unidad AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT u.es_centinela AND (f.t_id IS NULL OR p.t_id IS NULL);

INSERT INTO {schema}.col_unidadfuente (t_basket, fuente_administrativa, unidad)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    f.t_id,
    p.t_id
FROM tmp_col_unidadfuente AS u
JOIN {schema}.gc_fuenteadministrativa AS f
  ON f.local_id = u.fuente_administrativa AND f.espacio_de_nombres = 'DJANGO.GC_FUENTEADMINISTRATIVA'
JOIN {schema}.gc_predio AS p
  ON p.local_id = u.unidad AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT u.es_centinela;
