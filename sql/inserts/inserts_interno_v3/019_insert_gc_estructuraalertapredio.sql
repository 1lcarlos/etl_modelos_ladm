INSERT INTO {schema}.gc_estructuraalertapredio
(t_id, t_seq, descripcion_alerta, 
entidad_emisora_alerta, fecha_apertura_alerta, fecha_cierre_alerta, 
gc_predio_alerta_predio)
SELECT 
    nextval('{schema}.t_ili2db_seq'::regclass),  -- Generar nuevo t_id
    tmp.seq::bigint,                                       -- t_seq desde origen 
    CONCAT(tmp.estado,' ', tmp.tipo_documento_fuente),    -- descripcion_alerta (concatenaci��n de estado y tipo_documento_fuente)                                    
    COALESCE(tmp.ente_emisor,'Sin Especificar'),          -- entidad_emisora_alerta (usar valor o 'Sin Especificar' si es null)
    COALESCE(tmp.fecha_creacion::date, CURRENT_DATE), -- fecha_creacion (convertir a date y usar fecha actual si es null)
    NULL,                                          -- fecha_cierre_alerta (no disponible en origen, se deja NULL)
    gcp.t_id                                       -- gc_predio_alerta_predio (mapeo id origen -> t_id destino)
FROM tmp_gc_estructuraalertapredio tmp      
JOIN {schema}.gc_predio gcp
    ON gcp.local_id = tmp.gc_predio_alerta::text       -- Mapeo: id de origen (convertir a text para coincidir con local_id)