-- =============================================================================
-- Destino: col_miembros                    Origen: tmp_col_miembros
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'col_miembros', m.id, 'agrupacion/interesado',
       m.agrupacion || ' / ' || m.interesado_gc_interesado, 'RECHAZADO', 'Agrupacion o interesado no migrado'
FROM tmp_col_miembros AS m
LEFT JOIN {schema}.gc_agrupacioninteresados AS a
       ON a.local_id = m.agrupacion AND a.espacio_de_nombres = 'DJANGO.GC_AGRUPACIONINTERESADOS'
LEFT JOIN {schema}.gc_interesado AS i
       ON i.local_id = m.interesado_gc_interesado AND i.espacio_de_nombres = 'DJANGO.GC_INTERESADO'
WHERE NOT m.es_centinela AND (a.t_id IS NULL OR i.t_id IS NULL);

INSERT INTO {schema}.col_miembros (t_basket, interesado_gc_interesado, agrupacion, participacion)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    i.t_id,
    a.t_id,
    m.participacion::numeric
FROM tmp_col_miembros AS m
JOIN {schema}.gc_agrupacioninteresados AS a
  ON a.local_id = m.agrupacion AND a.espacio_de_nombres = 'DJANGO.GC_AGRUPACIONINTERESADOS'
JOIN {schema}.gc_interesado AS i
  ON i.local_id = m.interesado_gc_interesado AND i.espacio_de_nombres = 'DJANGO.GC_INTERESADO'
WHERE NOT m.es_centinela;
