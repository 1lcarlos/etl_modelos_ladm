SELECT 
gc.id as id_grupo_calificacion
, subtotal
, cct.text_code as clase_calificacion
, ect.text_code as conservacion
, cc.id as id_calificacion_convencional
FROM {schema}.cuc_grupocalificacion gc
left join {schema}.cuc_clasecalificaciontipo cct on gc.clase_calificacion = cct.id
left join {schema}.cuc_estadoconservaciontipo ect on gc.conservacion = ect.id
left join {schema}.cuc_calificacionconvencional cc on cc.id = gc.cuc_calificacion_convencional
WHERE cct.text_code not ilike  'Complemento_Industria' and subtotal is not null;