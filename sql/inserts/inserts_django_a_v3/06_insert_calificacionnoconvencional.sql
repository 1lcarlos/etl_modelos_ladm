INSERT INTO {schema}.gc_calificacionnoconvencional
(t_id,
t_ili_tid, 
tipo_anexo,  
gc_caracteristica_uconstruccion)
SELECT 
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(), 
    (SELECT t_id FROM {schema}.gc_anexotipo WHERE ilicode ILIKE '%' || cc.tipo_anexo || '%' ORDER BY t_id LIMIT 1) AS tipo_anexo,
    cu.t_id as gc_caracteristica_uconstruccion

    FROM tmp_calificacionnoconvencional cc
    JOIN {schema}.gc_caracteristicasunidadconstruccion cu ON cu.t_id = cc.gc_caracteristica_uconstruccion;
