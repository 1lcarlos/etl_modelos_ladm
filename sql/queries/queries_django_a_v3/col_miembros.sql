SELECT distinct on (m.id)
m.id as id_miembros
,m.participacion
,m.agrupacion as id_agrupacion
,m.interesado_gc_interesado as id_interesado
FROM {schema}.col_miembros m
join {schema}.gc_agrupacioninteresados ai on m.agrupacion = ai.id
join {schema}.gc_derecho d on ai.id = d.interesado_gc_agrupacioninteresados;