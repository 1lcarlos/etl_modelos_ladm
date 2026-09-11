SELECT
gc.id,
identificador, 
COALESCE(gu.text_code, 'Anexo') AS tipo_unidad_construccion,
case
    when total_plantas is null then 1
    else total_plantas
end as total_plantas,
COALESCE(uu.text_code,'Anexo.Kioscos') as uso,
case
	when anio_construccion is null then '1512'
	else anio_construccion
end as anio_construccion,
area as area_construida,
observacion as observaciones,
total_habitaciones, 
total_banios, 
total_locales 
FROM {schema}.gc_caracteristicasunidadconstruccion gc
LEFT JOIN {schema}.gc_unidadconstrucciontipo gu ON gu.id = gc.tipo_unidad_construccion
LEFT JOIN {schema}.gc_usouconstipo uu ON uu.id = gc.uso;


