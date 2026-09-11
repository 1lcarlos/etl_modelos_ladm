INSERT INTO {schema}.gc_interesado
(t_id,
t_basket,
t_ili_tid
, tipo_documento
, primer_nombre
, segundo_nombre
, primer_apellido
, segundo_apellido
, sexo
, autoreconocimientoetnico
, autoreconocimientocampesino
, razon_social
, nombre
, tipo_interesado
, numero_documento
, comienzo_vida_util_version
, espacio_de_nombres
, local_id)
SELECT 
   id_interesado
, (SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1)
, uuid_generate_v4()
, (SELECT t_id FROM {schema}.col_documentotipo dt WHERE dt.ilicode = i.tipo_documento AND dt.baseclass is not null LIMIT 1)
, primer_nombre
, segundo_nombre
, primer_apellido
, segundo_apellido
, (SELECT t_id FROM {schema}.gc_sexotipo WHERE ilicode = i.sexo LIMIT 1)
, (SELECT t_id FROM {schema}.gc_autoreconocimientoetnicotipo WHERE ilicode = i.grupo_etnico LIMIT 1)  as autoreconocimientoetnico
, false as autoreconocimientocampesino
, razon_social
, nombre
, (SELECT t_id FROM {schema}.col_interesadotipo WHERE ilicode = i.interesadotipo LIMIT 1) as tipointeresado
, documento_identidad as numero_documento
, comienzo_vida_util_version::timestamp
, espacio_de_nombres
--, estado_civil
--, tipo_verificado
, coalesce(i.id_interesado::varchar, i.local_id::varchar) as local_id
FROM tmp_interesado_agrupacion i
where i.id_interesado NOT IN (SELECT t_id FROM {schema}.gc_interesado);

