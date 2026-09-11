SELECT DISTINCT
case
	when i.espacio_de_nombres is null then 'gc_interesado'
	else i.espacio_de_nombres
end as espacio_de_nombres
, local_id
, case
	when i.comienzo_vida_util_version is null then now()
	else i.comienzo_vida_util_version
end as comienzo_vida_util_version
, nombre
, i.id as id_interesado
, documento_identidad
, primer_nombre
, segundo_nombre
, primer_apellido
, segundo_apellido
, razon_social
, ect.text_code as estado_civil
, geti.text_code as grupo_etnico
, CASE
    WHEN it.text_code = 'Persona_Juridica' THEN 'No_Aplica'
    WHEN sti.text_code IS NULL THEN 'Sin_Determinar'
    ELSE sti.text_code
END as sexo
, it.text_code as interesadotipo
, case
        when dt.text_code is null then 'Sin_Informacion'
        else dt.text_code        
 end as tipo_documento
, v.text_code as tipo_verificado
, i.id as local_id
FROM {schema}.gc_interesado i
left join {schema}.gc_estadociviltipo ect on i.estado_civil = ect.id  
left join {schema}.gc_grupoetnicotipo geti on i.grupo_etnico = geti.id
left join {schema}.gc_sexotipo sti on i.sexo = sti.id
left join {schema}.gc_interesadotipo it on i.tipo = it.id
left join {schema}.gc_interesadodocumentotipo dt on i.tipo_documento = dt.id
left join {schema}.gc_verificaciontipo v on i.verificado = v.id;