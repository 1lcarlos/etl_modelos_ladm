-- =============================================================================
-- Destino: col_uebaunit (terreno -> predio)
-- Origen : tmp_col_uebaunit_predio_terreno (enlace DIRECTO o por CODIGO_NPN)
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'col_uebaunit_terreno', u.id_terreno, 'baunit', u.id_predio, 'RECHAZADO',
       'Terreno o predio no migrado'
FROM tmp_col_uebaunit_predio_terreno AS u
LEFT JOIN {schema}.gc_terreno AS t ON t.local_id = u.id_terreno AND t.espacio_de_nombres = 'DJANGO.GC_TERRENO'
LEFT JOIN {schema}.gc_predio AS p ON p.local_id = u.id_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT u.es_centinela
  AND (t.t_id IS NULL OR p.t_id IS NULL)
UNION ALL
SELECT '{schema}', 'col_uebaunit_terreno', u.id_terreno, 'baunit', u.id_predio, 'DERIVADO',
       'Sin enlace en col_uebaunit de origen: se enlaza por codigo del terreno = numero predial'
FROM tmp_col_uebaunit_predio_terreno AS u
WHERE NOT u.es_centinela
  AND u.metodo_enlace = 'CODIGO_NPN'
UNION ALL
SELECT '{schema}', 'col_uebaunit_terreno', t.id, 'baunit', NULL, 'ADVERTENCIA',
       'Terreno sin predio asociado (se migra el terreno sin relacion)'
FROM tmp_terreno AS t
WHERE NOT t.es_centinela
  AND t.geometria IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM tmp_col_uebaunit_predio_terreno AS u
      WHERE NOT u.es_centinela AND u.id_terreno = t.id
  );

INSERT INTO {schema}.col_uebaunit (t_basket, ue_gc_terreno, baunit)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    t.t_id,
    p.t_id
FROM tmp_col_uebaunit_predio_terreno AS u
JOIN {schema}.gc_terreno AS t ON t.local_id = u.id_terreno AND t.espacio_de_nombres = 'DJANGO.GC_TERRENO'
JOIN {schema}.gc_predio AS p ON p.local_id = u.id_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT u.es_centinela;
