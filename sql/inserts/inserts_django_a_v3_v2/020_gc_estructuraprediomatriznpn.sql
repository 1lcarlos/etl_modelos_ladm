-- =============================================================================
-- Destino: gc_estructuraprediomatriznpn (estructura de la unidad predial)
-- Origen : tmp_predio_matriz_npn (gc_prediocopropiedad de Django)
-- Cada unidad predial registra el NPN y el area de terreno de su predio matriz.
-- PENDIENTE DE CONFIRMAR: que el dueno de la estructura sea la unidad y no la matriz.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_estructuraprediomatriznpn', m.id, 'gc_predio_predio_matriz_npn', m.unidad_predial,
       'RECHAZADO', 'Unidad predial no migrada'
FROM tmp_predio_matriz_npn AS m
LEFT JOIN {schema}.gc_predio AS pu ON pu.local_id = m.unidad_predial AND pu.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT m.es_centinela AND pu.t_id IS NULL;

INSERT INTO {schema}.gc_estructuraprediomatriznpn (
    t_basket,
    t_seq,
    numero_predial_nacional,
    area_catastral_terreno,
    gc_predio_predio_matriz_npn
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    row_number() OVER (PARTITION BY pu.t_id ORDER BY m.id::bigint) - 1,
    m.npn_matriz,
    m.area_terreno_matriz::numeric,
    pu.t_id
FROM tmp_predio_matriz_npn AS m
JOIN {schema}.gc_predio AS pu ON pu.local_id = m.unidad_predial AND pu.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT m.es_centinela;
