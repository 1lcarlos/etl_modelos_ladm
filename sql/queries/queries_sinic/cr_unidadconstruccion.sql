-- Query para extraer datos de gc_unidadconstruccion para migrar a cr_unidadconstruccion
-- Origen: {schema}.gc_unidadconstruccion
-- Destino: sinic2.cr_unidadconstruccion
-- Fecha: 2026-02-02

SELECT DISTINCT ON (uc.id)
    uc.id,
    COALESCE(uc.espacio_de_nombres, 'CR_UNIDADCONSTRUCCION') as espacio_de_nombres,
    COALESCE(uc.local_id, uc.id::varchar) as local_id,
    COALESCE(uc.comienzo_vida_util_version, NOW()) as comienzo_vida_util_version,
    uc.fin_vida_util_version,
    cpt.text_code as tipo_planta,
    --,COALESCE(uc.planta, 1) as planta_ubicacion,
    COALESCE(uc.planta_ubicacion, 1) as planta_ubicacion,
    uc.altura,
    uc.geometria,
    uc.etiqueta,
    uc.gc_construccion as construccion_id,
    uc.codigo,
    cuc.total_plantas,
    --cuc.total_sotanos,
    cuc.total_habitaciones,
    cuc.total_banios,
    cuc.total_locales,
    uct.text_code as tipo_unidad,
    uut.text_code as uso,
    uc.area,
    --uc.area_privada_construida,
    --uc.area_construida,
    uc.observacion,
    uc.gc_caracteristicasunidadconstruccion as caracteristicas_id
    ,'En_Rasante' as relacion_superficie
FROM {schema}.gc_unidadconstruccion uc
left join {schema}.gc_caracteristicasunidadconstruccion cuc on uc.gc_caracteristicasunidadconstruccion = cuc.id
LEFT JOIN {schema}.gc_construccionplantatipo cpt ON cuc.tipo_planta = cpt.id
LEFT JOIN {schema}.gc_unidadconstrucciontipo uct ON cuc.tipo_unidad_construccion = uct.id
LEFT JOIN {schema}.gc_usouconstipo uut ON cuc.uso = uut.id
ORDER BY uc.id;
