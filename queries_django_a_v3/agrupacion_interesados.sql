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
--,ai.id_antiguo
/* ,it.text_code as interesadotipo
, case
        when dt.text_code is null then 'Sin_Informacion'
        else dt.text_code        
 end as tipo_documento
 ,i.documento_identidad as numero_documento */
FROM {schema}.gc_agrupacioninteresados ai
left join {schema}.col_grupointeresadotipo git on ai.tipo = git.id
/* left join {schema}.col_miembros cm on ai.id = cm.agrupacion 
left join {schema}.gc_interesado i on cm.interesado_gc_interesado = i.id
left join {schema}.gc_interesadotipo it on i.tipo = it.id 
left join {schema}.gc_interesadodocumentotipo dt on i.tipo_documento = dt.id; */