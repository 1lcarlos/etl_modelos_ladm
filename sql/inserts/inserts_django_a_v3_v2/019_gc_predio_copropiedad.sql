-- =============================================================================
-- Destino: gc_predio_copropiedad            Origen: tmp_predio_copropiedad
-- Reglas:
--   * coeficiente: se copia (NULL si no existe en origen).
--   * area_catastral_terreno_coeficiente = coeficiente * area del terreno de la matriz
--     (NULL si falta alguno de los dos).
--   * area_privada_*: no existen en Django -> NULL.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_predio_copropiedad', c.id, 'matriz/unidad_predial',
       c.matriz || ' / ' || c.unidad_predial, 'RECHAZADO', 'Matriz o unidad predial no migrada'
FROM tmp_predio_copropiedad AS c
LEFT JOIN {schema}.gc_predio AS pm ON pm.local_id = c.matriz AND pm.espacio_de_nombres = 'DJANGO.GC_PREDIO'
LEFT JOIN {schema}.gc_predio AS pu ON pu.local_id = c.unidad_predial AND pu.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT c.es_centinela AND (pm.t_id IS NULL OR pu.t_id IS NULL);

INSERT INTO {schema}.gc_predio_copropiedad (
    t_basket,
    unidad_predial,
    matriz,
    coeficiente,
    area_catastral_terreno_coeficiente
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    pu.t_id,
    pm.t_id,
    c.coeficiente::numeric,
    round(c.coeficiente::numeric * c.area_terreno_matriz::numeric, 2)
FROM tmp_predio_copropiedad AS c
JOIN {schema}.gc_predio AS pm ON pm.local_id = c.matriz AND pm.espacio_de_nombres = 'DJANGO.GC_PREDIO'
JOIN {schema}.gc_predio AS pu ON pu.local_id = c.unidad_predial AND pu.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT c.es_centinela;
