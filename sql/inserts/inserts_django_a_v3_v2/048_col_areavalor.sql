-- =============================================================================
-- Destino: col_areavalor (estructura de gc_terreno y gc_unidadconstruccion)
-- Origen : tmp_area_valor (area registrada en Django)
-- tipo = Area_Alfanumerica (misma regla de las queries ajustadas; PENDIENTE DE CONFIRMAR).
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'col_areavalor', a.id_entidad, a.tipo_entidad, a.area, 'RECHAZADO',
       'Terreno o unidad de construccion no migrado'
FROM tmp_area_valor AS a
LEFT JOIN {schema}.gc_terreno AS t
       ON a.tipo_entidad = 'TERRENO' AND t.local_id = a.id_entidad AND t.espacio_de_nombres = 'DJANGO.GC_TERRENO'
LEFT JOIN {schema}.gc_unidadconstruccion AS u
       ON a.tipo_entidad = 'UNIDAD_CONSTRUCCION' AND u.local_id = a.id_entidad
      AND u.espacio_de_nombres = 'DJANGO.GC_UNIDADCONSTRUCCION'
WHERE NOT a.es_centinela AND t.t_id IS NULL AND u.t_id IS NULL;

INSERT INTO {schema}.col_areavalor (
    t_basket,
    t_seq,
    tipo,
    area,
    gc_terreno_area,
    gc_unidadconstruccion_area
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    0,
    etl_control.dominio('{schema}', 'col_areatipo', 'Area_Alfanumerica'),
    round(a.area::numeric, 1),
    t.t_id,
    u.t_id
FROM tmp_area_valor AS a
LEFT JOIN {schema}.gc_terreno AS t
       ON a.tipo_entidad = 'TERRENO' AND t.local_id = a.id_entidad AND t.espacio_de_nombres = 'DJANGO.GC_TERRENO'
LEFT JOIN {schema}.gc_unidadconstruccion AS u
       ON a.tipo_entidad = 'UNIDAD_CONSTRUCCION' AND u.local_id = a.id_entidad
      AND u.espacio_de_nombres = 'DJANGO.GC_UNIDADCONSTRUCCION'
WHERE NOT a.es_centinela
  AND (t.t_id IS NOT NULL OR u.t_id IS NOT NULL);
