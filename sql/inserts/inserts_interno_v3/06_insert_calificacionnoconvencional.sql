INSERT INTO {schema}.gc_calificacionnoconvencional
(t_id, 
t_ili_tid, 
tipo_anexo,  
gc_caracteristica_uconstruccion)
SELECT 
    nextval('{schema}.t_ili2db_seq'::regclass),  
    uuid_generate_v4(), 
    ca.t_id,
    cu.t_id
    FROM tmp_calificacionnoconvencional cc
    JOIN {schema}.gc_anexotipo ca ON ca.ilicode ILIKE '%' || cc.tipo_anexo || '%'
    JOIN {schema}.gc_caracteristicasunidadconstruccion cu ON cu.t_id = cc.gc_caracteristica_uconstruccion;
