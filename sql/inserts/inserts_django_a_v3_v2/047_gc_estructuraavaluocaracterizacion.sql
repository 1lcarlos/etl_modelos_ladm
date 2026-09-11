-- =============================================================================
-- Destino: gc_estructuraavaluocaracterizacion
-- Origen : tmp_avaluo_caracterizacion (avaluos de unidades de construccion en Django)
-- Requiere map_caracteristica (040). fecha_avaluo = vigencia.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_estructuraavaluocaracterizacion', a.id,
       'gc_crctrstcdcnstrccion_avaluo_caracterizacion', a.id_caracteristica, 'RECHAZADO',
       'Caracteristica no migrada'
FROM tmp_avaluo_caracterizacion AS a
LEFT JOIN map_caracteristica AS m ON m.id_origen = a.id_caracteristica::bigint
WHERE NOT a.es_centinela AND m.t_id IS NULL;

INSERT INTO {schema}.gc_estructuraavaluocaracterizacion (
    t_basket,
    t_seq,
    fecha_avaluo,
    avaluo_catastral,
    gc_crctrstcdcnstrccion_avaluo_caracterizacion
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    row_number() OVER (PARTITION BY m.t_id ORDER BY a.vigencia::date DESC NULLS LAST, a.id::bigint) - 1,
    a.vigencia::date,
    a.avaluo_catastral::numeric,
    m.t_id
FROM tmp_avaluo_caracterizacion AS a
JOIN map_caracteristica AS m ON m.id_origen = a.id_caracteristica::bigint
WHERE NOT a.es_centinela;
