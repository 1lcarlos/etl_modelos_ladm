SELECT 'gc_terreno' as espacio_de_nombres, 
id, 
case
	when t.comienzo_vida_util_version is null then now()
	else t.comienzo_vida_util_version
end as comienzo_vida_util_version,
fin_vida_util_version, 
codigo, 
etiqueta, 
observacion, 
geometria
FROM {schema}.gc_terreno t;