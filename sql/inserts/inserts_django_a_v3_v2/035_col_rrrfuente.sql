-- =============================================================================
-- Destino: col_rrrfuente (fuente administrativa -> derecho catastral)
-- Origen : tmp_col_rrrfuente
-- Las restricciones no se migran en este flujo: si la fila apunta a una, se reporta.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'col_rrrfuente', r.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_col_rrrfuente AS r
LEFT JOIN {schema}.gc_fuenteadministrativa AS f
       ON f.local_id = r.fuente_administrativa AND f.espacio_de_nombres = 'DJANGO.GC_FUENTEADMINISTRATIVA'
LEFT JOIN {schema}.gc_derechocatastral AS d
       ON d.local_id = r.rrr_gc_derecho AND d.espacio_de_nombres = 'DJANGO.GC_DERECHO'
CROSS JOIN LATERAL (VALUES
    ('rrr_gc_restriccion', r.rrr_gc_restriccion, 'NO_MIGRADO', 'Las restricciones no se migran en este flujo',
     r.rrr_gc_restriccion IS NOT NULL),
    ('fuente/derecho', r.fuente_administrativa || ' / ' || r.rrr_gc_derecho, 'RECHAZADO',
     'Fuente administrativa o derecho no migrado',
     r.rrr_gc_derecho IS NOT NULL AND (f.t_id IS NULL OR d.t_id IS NULL))
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT r.es_centinela AND x.aplica;

INSERT INTO {schema}.col_rrrfuente (t_basket, fuente_administrativa, rrr_gc_derechocatastral)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    f.t_id,
    d.t_id
FROM tmp_col_rrrfuente AS r
JOIN {schema}.gc_fuenteadministrativa AS f
  ON f.local_id = r.fuente_administrativa AND f.espacio_de_nombres = 'DJANGO.GC_FUENTEADMINISTRATIVA'
JOIN {schema}.gc_derechocatastral AS d
  ON d.local_id = r.rrr_gc_derecho AND d.espacio_de_nombres = 'DJANGO.GC_DERECHO'
WHERE NOT r.es_centinela;
