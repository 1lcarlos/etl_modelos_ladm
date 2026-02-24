INSERT INTO
    ric.ric_tramitecatastral (
        t_id,
        t_ili_tid,
        clasificacion_mutacion,
        numero_resolucion,
        fecha_resolucion,
        fecha_radicacion
    )
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    --id_tramite::bigint,
    uuid_generate_v4 (),      
    case 
        when t.tramite_tipo ilike '%primera%' then (SELECT t_id FROM ric.ric_mutaciontipo
         WHERE ilicode ILIKE '%primera%'
         LIMIT 1)
         when t.tramite_tipo ilike '%segunda%' then (SELECT t_id FROM ric.ric_mutaciontipo
         WHERE ilicode ILIKE '%segunda%' 
         LIMIT 1)
         when t.tramite_tipo ilike '%tercera%' then (SELECT t_id FROM ric.ric_mutaciontipo
         WHERE ilicode ILIKE '%tercera%' 
         LIMIT 1)
         when t.tramite_tipo ilike '%cuarta%' then (SELECT t_id FROM ric.ric_mutaciontipo
         WHERE ilicode ILIKE '%cuarta%' 
         LIMIT 1)
         when t.tramite_tipo ilike '%quinta%' then (SELECT t_id FROM ric.ric_mutaciontipo
         WHERE ilicode ILIKE '%quinta%' 
         LIMIT 1)
         when t.tramite_tipo ilike '%Rectificación%' then (SELECT t_id FROM ric.ric_mutaciontipo
         WHERE ilicode ILIKE '%Rectificaciones%'  
         LIMIT 1)
         when t.tramite_tipo ilike '%Cancelación%' then (SELECT t_id FROM ric.ric_mutaciontipo         
         WHERE ilicode ILIKE '%Cancelacion%' 
         LIMIT 1)
         when t.tramite_tipo ilike '%Procedimientos%' then (SELECT t_id FROM ric.ric_mutaciontipo         
         WHERE ilicode ILIKE '%Rectificaciones%' 
         LIMIT 1)
    end as clasificacion_mutacion,    
    resolution_number,     
    fecha_radicado::date,
    resolution_date::date
FROM ric.tmp_tramite t;


