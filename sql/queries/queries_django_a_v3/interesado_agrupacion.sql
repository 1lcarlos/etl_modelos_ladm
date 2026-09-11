SELECT DISTINCT
    case
	when i.espacio_de_nombres is null then 'gc_interesado'
	else i.espacio_de_nombres
end as espacio_de_nombres
, i.local_id
, case
	when i.comienzo_vida_util_version is null then now()
	else i.comienzo_vida_util_version
end as comienzo_vida_util_version
, i.nombre
, i.id as id_interesado
, i.documento_identidad
, i.primer_nombre
, i.segundo_nombre
, i.primer_apellido
, i.segundo_apellido
, i.razon_social
, ect.text_code as estado_civil
, autrec.text_code as grupo_etnico
, CASE
    WHEN it.text_code = 'Persona_Juridica' THEN 'No_Aplica'
    WHEN st.text_code IS NULL THEN 'Sin_Determinar'
    ELSE st.text_code
END as sexo
, it.text_code as interesadotipo
, case
        when dt.text_code is null then 'Sin_Informacion'
        else dt.text_code        
 end as tipo_documento
, v.text_code as tipo_verificado
FROM {schema}.gc_interesado i
LEFT JOIN {schema}.gc_estadociviltipo ect ON i.estado_civil = ect.id
LEFT JOIN {schema}.gc_grupoetnicotipo autrec ON i.grupo_etnico = autrec.id
LEFT JOIN {schema}.gc_sexotipo st ON i.sexo = st.id
LEFT JOIN {schema}.gc_interesadotipo it ON i.tipo = it.id
LEFT JOIN {schema}.gc_interesadodocumentotipo  dt ON i.tipo_documento = dt.id
join {schema}.col_miembros m on i.id = m.interesado_gc_interesado
join {schema}.gc_derecho dc on m.agrupacion  = dc.interesado_gc_agrupacioninteresados
left join {schema}.gc_verificaciontipo v on i.verificado = v.id