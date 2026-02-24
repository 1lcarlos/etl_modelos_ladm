INSERT INTO {schema}.gc_calificacionconvencional
(t_id, 
t_ili_tid, 
tipo_calificar, 
total_calificacion, 
gc_caracteristica_uconstruccion)
SELECT 
    --nextval('{schema}.t_ili2db_seq'::regclass), 
    id_calificacion_convencional, 
    uuid_generate_v4(), 
    ca.t_id as tipo_calificar, 
    cc.total_calificacion::numeric,
    cu.t_id as gc_caracteristica_uconstruccion
    FROM tmp_calificacionconvencional cc
    JOIN {schema}.gc_calificartipo ca ON ca.ilicode ILIKE '%' || cc.tipo_calificar || '%'
    JOIN {schema}.gc_caracteristicasunidadconstruccion cu ON cu.t_id = cc.gc_caracteristica_uconstruccion;
