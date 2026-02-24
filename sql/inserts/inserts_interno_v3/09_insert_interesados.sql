INSERT INTO {schema}.gc_interesado
(t_id, 
t_ili_tid, 
tipo_documento, 
primer_nombre, 
segundo_nombre, 
primer_apellido, 
segundo_apellido, 
sexo, 
autoreconocimientoetnico, 
autoreconocimientocampesino, 
razon_social, 
nombre, 
tipo_interesado, 
numero_documento, 
comienzo_vida_util_version, 
espacio_de_nombres, 
local_id)
SELECT 
--nextval('{schema}.t_ili2db_seq'::regclass)
  id_interesado
, uuid_generate_v4()
, dt.t_id as tipo_documento
, primer_nombre
, segundo_nombre
, primer_apellido
, segundo_apellido
, sti.t_id as sexo
, autrec.t_id as autoreconocimientoetnico
, false as autoreconocimientocampesino
, razon_social
, nombre
, it.t_id as tipointeresado
, documento_identidad as numero_documento
, comienzo_vida_util_version::timestamp
, espacio_de_nombres
--, estado_civil
--, tipo_verificado
, COALESCE(local_id, id_interesado::varchar)
FROM tmp_interesado i
join {schema}.gc_sexotipo sti on sti.ilicode ILIKE '%' ||  i.sexo || '%'  
join {schema}.col_interesadotipo it on it.ilicode ILIKE '%' ||  i.interesadotipo || '%'  
join {schema}.col_documentotipo dt on dt.ilicode ILIKE '%' ||  i.tipo_documento || '%'  
left join {schema}.gc_autoreconocimientoetnicotipo autrec on autrec.ilicode ILIKE '%' ||  i.grupo_etnico || '%' 
WHERE dt.baseclass is not null
