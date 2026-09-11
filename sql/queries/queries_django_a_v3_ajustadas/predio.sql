-- Consulta para extraer datos de la tabla gc_predio con joins a tablas relacionadas

SELECT distinct 
case
	when p.espacio_de_nombres is null then 'gc_predio'
	else p.espacio_de_nombres
end as espacio_de_nombres, 
local_id, 
case
	when p.comienzo_vida_util_version is null then now()
	else p.comienzo_vida_util_version
end as comienzo_vida_util_version, 
fin_vida_util_version, 
p.id, 
p.departamento, 
p.municipio, 
codigo_orip, 
case 
    when p.matricula_inmobiliaria is null then 1
    when length(p.matricula_inmobiliaria ) >= 6 then 1
    when p.matricula_inmobiliaria ~ '^[0-9]+$' and p.matricula_inmobiliaria::numeric = 0 then 1
    when p.matricula_inmobiliaria ~ '^[0-9]+$' then p.matricula_inmobiliaria::numeric
    else 1
end as matricula_inmobiliaria, 
numero_predial as numero_predial_nacional, 
numero_predial_anterior, 
case 
	when p.nupre is null then 'BBK00000'
	when p.nupre = '' then 'BBK00000'	
	else p.nupre	
end as nupre,
case
	when area is null then 0		
	else area
end as area_catastral_terreno, 
dalc.area_registral_m2 as area_registral,
case 
	when clst.text_code = 'Urbano' then ci.vigencia_urbana 
	when clst.text_code = 'Rural' then ci.vigencia_rural 
	when clst.text_code = 'Expansion_Urbana' then ci.vigencia_rural 
end as vigencia_actualizacion_catastral,
'Activo' as estado,
area_construida, nombre, 
'Estadistica' as tipo ,
cst.text_code as categoria_suelo, 
clst.text_code as clase_suelo, 
cpt.text_code as condicion_predio, 
det.text_code  as destinacion_economica, 
case
	when pt.text_code is null then 'Privado'
	else pt.text_code
end as tipo_predio
FROM {schema}.gc_predio p
left join {schema}.gc_categoriasuelotipo cst on p.categoria_suelo = cst.id 
left join {schema}.gc_clasesuelotipo clst on p.clase_suelo = clst.id
left join {schema}.gc_condicionprediotipo cpt on p.condicion_predio = cpt.id 
left join {schema}.gc_destinacioneconomicatipo det on p.destinacion_economica = det.id 
left join {schema}.gc_prediotipo pt  on p.tipo = pt.id 
left join {schema}.dlc_datosadicionaleslevantamientocatastral dalc on p.id = dalc.gc_predio
left join public.cadaster_information ci on ci.municipio = concat(p.departamento,p.municipio);