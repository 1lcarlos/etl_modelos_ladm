INSERT INTO {schema}.gc_caracteristicasunidadconstruccion
(t_id, 
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
    cu.id, 
    uuid_generate_v4(),
    cu.identificador,
    gu.t_id as tipo_unidad_construccion,
    cu.total_plantas,
    uu.t_id as uso,
    cu.anio_construccion,
    cu.area_construida::numeric,
    'Bueno' AS estado_conservacion,
    cu.observaciones,
    cu.total_habitaciones::numeric,
    cu.total_banios::numeric,
    cu.total_locales::numeric
    FROM tmp_caracteristicasunidadconstruccion cu
    JOIN {schema}.gc_unidadconstrucciontipo gu ON gu.ilicode = cu.tipo_unidad_construccion  
    JOIN {schema}.gc_usouconstipo uu ON uu.ilicode = cu.uso;

    