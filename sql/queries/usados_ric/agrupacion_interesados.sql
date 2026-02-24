SELECT 
ai.espacio_de_nombres
,ai.local_id
,case
	when ai.comienzo_vida_util_version is null then now()
	else ai.comienzo_vida_util_version
end as comienzo_vida_util_version
,ai.id as id_agrupacion
,ai.nombre
, case
    when git.text_code is null then 'Grupo_Civil'
    else git.text_code
end as tipo_agrupacion 
FROM {schema}.gc_agrupacioninteresados ai
left join {schema}.col_grupointeresadotipo git on ai.tipo = git.id
