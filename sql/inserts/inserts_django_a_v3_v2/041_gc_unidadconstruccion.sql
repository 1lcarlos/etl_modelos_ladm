-- =============================================================================
-- Destino: gc_unidadconstruccion           Origen: tmp_unidadconstruccion
-- Reglas (decision 2026-09-11: todas las unidades con geometria):
--   * local_id = id de Django; espacio_de_nombres = 'DJANGO.GC_UNIDADCONSTRUCCION'.
--   * Sin caracteristica o construccion asociada: se migra igual (FK en NULL).
--   * altura: entero entre 1 y 1000; fuera de rango -> NULL + incidencia.
--   * planta_ubicacion: entre 0 y 300; fuera de rango -> NULL + incidencia.
--   * fin_vida_util_version se copia (antes se forzaba la fecha de ejecucion y todas
--     las unidades quedaban como versiones cerradas).
--   * El area va a 048_col_areavalor; observacion no tiene columna en v3.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_unidadconstruccion', u.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_unidadconstruccion AS u
LEFT JOIN map_caracteristica AS m ON m.id_origen = u.id_caracteristica::bigint
CROSS JOIN LATERAL (VALUES
    ('cr_caracteristicasunidadconstruccion', u.id_caracteristica, 'NULIFICADO',
     'Sin caracteristica migrada', m.t_id IS NULL),
    ('altura', u.altura, 'NULIFICADO', 'Fuera del rango 1..1000 de v3',
     u.altura IS NOT NULL AND NOT (u.altura::numeric BETWEEN 1 AND 1000)),
    ('planta_ubicacion', u.planta_ubicacion, 'NULIFICADO', 'Fuera del rango 0..300 de v3',
     u.planta_ubicacion IS NOT NULL AND NOT (u.planta_ubicacion::integer BETWEEN 0 AND 300)),
    ('tipo_planta', u.tipo_planta, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     u.tipo_planta IS NOT NULL AND etl_control.dominio('{schema}', 'gc_construccionplantatipo', u.tipo_planta) IS NULL),
    ('observacion', left(u.observacion, 255), 'NO_MIGRADO', 'El modelo destino no tiene este atributo',
     etl_control.texto(u.observacion) IS NOT NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT u.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_unidadconstruccion (
    t_basket,
    tipo_planta,
    planta_ubicacion,
    altura,
    geometria,
    codigo,
    cr_caracteristicasunidadconstruccion,
    etiqueta,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    etl_control.dominio('{schema}', 'gc_construccionplantatipo', u.tipo_planta),
    CASE WHEN u.planta_ubicacion::integer BETWEEN 0 AND 300 THEN u.planta_ubicacion::integer END,
    CASE WHEN u.altura::numeric BETWEEN 1 AND 1000 THEN round(u.altura::numeric)::integer END,
    u.geometria::geometry,
    etl_control.texto(u.codigo),
    m.t_id,
    etl_control.texto(u.etiqueta),
    COALESCE(u.comienzo_vida_util_version::timestamp, now()::timestamp),
    u.fin_vida_util_version::timestamp,
    'DJANGO.GC_UNIDADCONSTRUCCION',
    u.id
FROM tmp_unidadconstruccion AS u
LEFT JOIN map_caracteristica AS m ON m.id_origen = u.id_caracteristica::bigint
WHERE NOT u.es_centinela;
