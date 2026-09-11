-- =============================================================================
-- Destino: gc_puntocontrol                 Origen: tmp_punto_control
-- Mismas reglas de gc_puntolindero. puntotipo usa el mismo mapeo (Catastro.*).
-- PENDIENTE DE CONFIRMAR: si para puntos de control el puntotipo debe ser 'Control'.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_puntocontrol', pc.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_punto_control AS pc
CROSS JOIN LATERAL (VALUES
    ('geometria', NULL, 'RECHAZADO', 'Punto sin geometria', pc.geometria IS NULL),
    ('puntotipo', pc.punto_tipo, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     pc.punto_tipo IS NOT NULL AND etl_control.dominio('{schema}', 'col_puntotipo', pc.punto_tipo) IS NULL),
    ('metodoproduccion', pc.metodo_produccion, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     pc.metodo_produccion IS NOT NULL AND etl_control.dominio('{schema}', 'col_metodoproducciontipo', pc.metodo_produccion) IS NULL),
    ('tipo_punto_control', pc.tipo_punto_control, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     pc.tipo_punto_control IS NOT NULL AND etl_control.dominio('{schema}', 'gc_puntocontroltipo', pc.tipo_punto_control) IS NULL),
    ('exactitud_vertical', pc.exactitud_vertical, 'NO_MIGRADO', 'El modelo destino no tiene este atributo',
     pc.exactitud_vertical IS NOT NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT pc.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_puntocontrol (
    t_basket,
    id_punto_control,
    puntotipo,
    tipo_punto_control,
    exactitud_horizontal,
    metodoproduccion,
    geometria,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    pc.id_punto,
    etl_control.dominio('{schema}', 'col_puntotipo', pc.punto_tipo),
    etl_control.dominio('{schema}', 'gc_puntocontroltipo', pc.tipo_punto_control),
    pc.exactitud_horizontal::numeric,
    etl_control.dominio('{schema}', 'col_metodoproducciontipo', pc.metodo_produccion),
    pc.geometria::geometry,
    COALESCE(pc.comienzo_vida_util_version::timestamp, now()::timestamp),
    pc.fin_vida_util_version::timestamp,
    'DJANGO.GC_PUNTOCONTROL',
    pc.id
FROM tmp_punto_control AS pc
WHERE NOT pc.es_centinela
  AND pc.geometria IS NOT NULL;
