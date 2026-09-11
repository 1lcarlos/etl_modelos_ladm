SELECT
distinct on (cc.id)
cc.id as id_caracteristicasunidadconstruccion,
cc.identificador, 
COALESCE(gu.text_code, 'Anexo') AS tipo_unidad_construccion,
case
    when cc.total_plantas is null then 1
    else cc.total_plantas
end as total_plantas,
COALESCE(uu.text_code,'Anexo.Ramadas_Cobertizos_Caneyes') as uso,
case
	when cc.anio_construccion is null then '1512'
	else cc.anio_construccion
end as anio_construccion,
cc.area as area_construida,
cc.observacion as observaciones,
cc.total_habitaciones, 
cc.total_banios, 
cc.total_locales 
FROM {schema}.gc_caracteristicasunidadconstruccion cc
LEFT JOIN {schema}.gc_unidadconstrucciontipo gu ON gu.id = cc.tipo_unidad_construccion
LEFT JOIN {schema}.gc_usouconstipo uu ON uu.id = cc.uso
join {schema}.gc_unidadconstruccion u on u.gc_caracteristicasunidadconstruccion  = cc.id;