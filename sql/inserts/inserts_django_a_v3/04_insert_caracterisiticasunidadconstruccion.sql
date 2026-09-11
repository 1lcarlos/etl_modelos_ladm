INSERT INTO {schema}.gc_caracteristicasunidadconstruccion
(t_id,
t_basket,
t_ili_tid, 
identificador, 
tipo_unidad_construccion, 
total_plantas, 
uso, 
anio_construccion, 
area_construida, 
estado_conservacion, 
observaciones, 
total_habitaciones, 
total_banios, 
total_locales)
SELECT
    cu.id_caracteristicasunidadconstruccion::bigint,
    (SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4(),
    cu.identificador,
    COALESCE(
        (SELECT t_id FROM {schema}.gc_unidadconstrucciontipo WHERE ilicode ILIKE '%' || cu.tipo_unidad_construccion || '%' ORDER BY t_id LIMIT 1),
        (SELECT t_id FROM {schema}.gc_unidadconstrucciontipo ORDER BY t_id LIMIT 1)
    ) AS tipo_unidad_construccion,
    cu.total_plantas,
    COALESCE(
        (SELECT t_id FROM {schema}.gc_usouconstipo WHERE ilicode ILIKE '%' || cu.uso || '%' ORDER BY t_id LIMIT 1),
        (SELECT t_id FROM {schema}.gc_usouconstipo ORDER BY t_id LIMIT 1)
    ) AS uso,
    cu.anio_construccion,
    COALESCE(cu.area_construida::numeric, 0),
    'Bueno' AS estado_conservacion,
    cu.observaciones,
    cu.total_habitaciones::numeric,
    cu.total_banios::numeric,
    cu.total_locales::numeric
    FROM tmp_caracteristicasunidadconstruccion cu;


    