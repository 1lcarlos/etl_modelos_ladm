SELECT DISTINCT ON (cuc.id)
    cuc.id,
    COALESCE(cuc.espacio_de_nombres, 'CR_CARACTERISTICAS') as espacio_de_nombres,
    COALESCE(cuc.local_id, cuc.id::varchar) as local_id,
    COALESCE(cuc.comienzo_vida_util_version, NOW()) as comienzo_vida_util_version,
    cuc.fin_vida_util_version,
    cuc.identificador,
    uct.text_code as tipo_unidad_construccion,
    dct.text_code as tipo_dominio,
    uut.text_code as uso,
    COALESCE(cuc.total_habitaciones, 0) as total_habitaciones,
    COALESCE(cuc.total_banios, 0) as total_banios,
    COALESCE(cuc.total_locales, 0) as total_locales,
    --COALESCE(cuc.total_pisos, 1) as total_pisos,
    COALESCE(cuc.total_plantas, 1) as total_pisos,
    --COALESCE(cuc.total_sotanos, 0) as total_sotanos,
    --COALESCE(cuc.total_mezanine, 0) as total_mezanine,
    cuc.area::numeric(16, 2),
    cuc.anio_construccion,
    cuc.area_privada_construida as area_construida,
    cuc.puntaje,
    --cuc.puntuacion,
    cuc.codigo, 
    cuc.observacion
    --,COALESCE(cuc.area_privada_construida, 0) as area_privada_construida,
    --COALESCE(cuc.area_construida, 0) as area_construida,
    --cuc.gc_construccion as construccion_id,
    --cuc.numero_predial
FROM {schema}.gc_caracteristicasunidadconstruccion cuc
LEFT JOIN {schema}.gc_unidadconstrucciontipo uct ON cuc.tipo_unidad_construccion = uct.id
LEFT JOIN {schema}.gc_dominioconstrucciontipo dct ON cuc.tipo_dominio = dct.id
LEFT JOIN {schema}.gc_usouconstipo uut ON cuc.uso = uut.id
ORDER BY cuc.id;