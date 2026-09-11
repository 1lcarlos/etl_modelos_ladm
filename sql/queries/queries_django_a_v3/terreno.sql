SELECT 
'gc_terreno' as espacio_de_nombres, 
t.id as id_terreno, 
case
	when t.comienzo_vida_util_version is null then now()
	else t.comienzo_vida_util_version
end as comienzo_vida_util_version,
t.fin_vida_util_version as fin_vida_util_version,
t.codigo as codigo,
t.etiqueta as etiqueta,
t.observacion as observacion,
t.geometria as geometria
FROM {schema}.gc_terreno t
join {schema}.col_uebaunit ue on t.id = ue.ue_gc_terreno
join {schema}.gc_predio p on p.id = ue.unidad;