-- =============================================================================
-- Destino: gc_datosmatriz                  Origen: tmp_datos_matriz (gc_datosphcondominio)
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_datosmatriz', d.id, 'cr_predio', d.gc_predio, 'RECHAZADO', 'Predio no migrado'
FROM tmp_datos_matriz AS d
LEFT JOIN {schema}.gc_predio AS p ON p.local_id = d.gc_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT d.es_centinela AND p.t_id IS NULL;

INSERT INTO {schema}.gc_datosmatriz (
    t_basket,
    area_total_terreno,
    area_total_terreno_privada,
    area_total_terreno_comun,
    area_total_construida,
    area_total_construida_privada,
    area_total_construida_comun,
    numero_torres,
    total_unidades_privadas,
    cr_predio
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    d.area_total_terreno::numeric,
    d.area_total_terreno_privada::numeric,
    d.area_total_terreno_comun::numeric,
    d.area_total_construida::numeric,
    d.area_total_construida_privada::numeric,
    d.area_total_construida_comun::numeric,
    d.numero_torres::integer,
    d.total_unidades_privadas::integer,
    p.t_id
FROM tmp_datos_matriz AS d
JOIN {schema}.gc_predio AS p ON p.local_id = d.gc_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT d.es_centinela;
