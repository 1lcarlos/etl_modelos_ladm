INSERT INTO {schema}.cr_estructuraavaluo (
    t_id,    
    fecha_avaluo,              -- REQUIRED (NOT NULL)
    avaluo_catastral,          -- REQUIRED (NOT NULL)
    autoestimacion,            -- REQUIRED (NOT NULL)
    --vigencia,
    --por_decreto,
    --descripcion,
    sinic_predio_avaluo
)
SELECT
    --id_avaluo,                                       -- t_id (usar id de origen como t_id destino)
    nextval('{schema}.t_ili2db_seq'::regclass),  -- Generar nuevo t_id                                      
    COALESCE(tmp.vigencia::date, CURRENT_DATE),         -- fecha_avaluo (usar vigencia o fecha actual)
    COALESCE(tmp.avaluo_catastral::numeric, 0),            -- avaluo_catastral (NOT NULL, default 0 si es null)
    'false'::boolean,                                         -- autoestimacion (NOT NULL, default FALSE)
    --tmp.vigencia::date,                                  -- vigencia
    --tmp.por_decreto::boolean,                      -- por_decreto (convertir boolean a integer: true→1, false→0)
    --tmp.descripcion,                               -- descripcion
    gcp.t_id                                       -- gc_predio_avaluo (mapeo id origen -> t_id destino)
FROM tmp_gc_estructuraavaluo tmp
JOIN {schema}.sinic_predio gcp
    ON gcp.local_id = tmp.gc_predio_avaluo::text       -- Mapeo: id de origen est� en local_id de destino
--WHERE tmp.gc_predio_avaluo IS NOT NULL;          -- Solo insertar si hay predio asociado
