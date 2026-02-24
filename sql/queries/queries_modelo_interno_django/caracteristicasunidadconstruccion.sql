SELECT
distinct on(cc.id)
cc.id,
identificador, 
COALESCE(gu.text_code, 'Residencial') as tipo_unidad_construccion,        
case
    when total_plantas is null then 1
    else total_plantas
end as total_plantas,
COALESCE(uu.text_code,'Residencial.Vivienda_Hasta_3_Pisos') as uso,        
case
	when anio_construccion is null then '1512'
	else anio_construccion
end as anio_construccion,
cc.area as area_construida,
cc.observacion as observaciones,
total_habitaciones, 
total_banios, 
total_locales 
FROM {schema}.gc_caracteristicasunidadconstruccion cc
LEFT JOIN {schema}.gc_unidadconstrucciontipo gu ON gu.id = cc.tipo_unidad_construccion
LEFT JOIN {schema}.gc_usouconstipo uu ON uu.id = cc.uso
LEFT JOIN {schema}.gc_unidadconstruccion uc ON uc.gc_caracteristicasunidadconstruccion = cc.id
WHERE uc.geometria IS NOT NULL;


