-- =============================================================================
-- Destino: gc_agrupacioninteresados        Origen: tmp_agrupacion_interesados
-- Reglas: local_id = id de Django; espacio_de_nombres = 'DJANGO.GC_AGRUPACIONINTERESADOS'.
--         tipo sin dato -> NULL (antes se forzaba Grupo_Civil).
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_agrupacioninteresados', a.id, 'tipo', a.tipo, 'NULIFICADO',
       'Valor sin equivalente en el dominio destino'
FROM tmp_agrupacion_interesados AS a
WHERE NOT a.es_centinela
  AND a.tipo IS NOT NULL
  AND etl_control.dominio('{schema}', 'col_grupointeresadotipo', a.tipo) IS NULL;

INSERT INTO {schema}.gc_agrupacioninteresados (
    t_basket,
    tipo,
    nombre,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    etl_control.dominio('{schema}', 'col_grupointeresadotipo', a.tipo),
    etl_control.texto(a.nombre),
    COALESCE(a.comienzo_vida_util_version::timestamp, now()::timestamp),
    a.fin_vida_util_version::timestamp,
    'DJANGO.GC_AGRUPACIONINTERESADOS',
    a.id
FROM tmp_agrupacion_interesados AS a
WHERE NOT a.es_centinela;
