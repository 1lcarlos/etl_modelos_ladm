INSERT INTO {schema}.gc_tramitecatastral (
    t_id,
    t_ili_tid,
    clasificacion_mutacion,
    numero_resolucion,
    fecha_resolucion,
    fecha_radicacion,
    fecha_inscripcion
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    CASE
        WHEN tmp.tipo_mutacion ILIKE '%primera%'       THEN (SELECT t_id FROM {schema}.gc_mutaciontipo WHERE ilicode ILIKE '%primera%'        ORDER BY t_id LIMIT 1)
        WHEN tmp.tipo_mutacion ILIKE '%segunda%'       THEN (SELECT t_id FROM {schema}.gc_mutaciontipo WHERE ilicode ILIKE '%segunda%'        ORDER BY t_id LIMIT 1)
        WHEN tmp.tipo_mutacion ILIKE '%tercera%'       THEN (SELECT t_id FROM {schema}.gc_mutaciontipo WHERE ilicode ILIKE '%tercera%'        ORDER BY t_id LIMIT 1)
        WHEN tmp.tipo_mutacion ILIKE '%cuarta%'        THEN (SELECT t_id FROM {schema}.gc_mutaciontipo WHERE ilicode ILIKE '%cuarta%'         ORDER BY t_id LIMIT 1)
        WHEN tmp.tipo_mutacion ILIKE '%quinta%'        THEN (SELECT t_id FROM {schema}.gc_mutaciontipo WHERE ilicode ILIKE '%quinta%'         ORDER BY t_id LIMIT 1)
        WHEN tmp.tipo_mutacion ILIKE '%Rectificaci%'   THEN (SELECT t_id FROM {schema}.gc_mutaciontipo WHERE ilicode ILIKE '%Rectificaciones%' ORDER BY t_id LIMIT 1)
        WHEN tmp.tipo_mutacion ILIKE '%Cancelaci%'     THEN (SELECT t_id FROM {schema}.gc_mutaciontipo WHERE ilicode ILIKE '%Cancelacion%'    ORDER BY t_id LIMIT 1)
        WHEN tmp.tipo_mutacion ILIKE '%Procedimientos%' THEN (SELECT t_id FROM {schema}.gc_mutaciontipo WHERE ilicode ILIKE '%Rectificaciones%' ORDER BY t_id LIMIT 1)
        ELSE (SELECT t_id FROM {schema}.gc_mutaciontipo ORDER BY t_id LIMIT 1)
    END,
    tmp.numero_resolucion,
    tmp.fecha_resolucion::date,
    tmp.fecha_radicacion::date,
    tmp.fecha_resolucion::date  -- fecha_inscripcion = fecha_resolucion
FROM tmp_gc_tramitecatastral tmp
WHERE tmp.numero_resolucion IS NOT NULL;
