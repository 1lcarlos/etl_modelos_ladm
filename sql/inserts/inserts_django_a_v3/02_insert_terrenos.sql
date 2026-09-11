INSERT INTO {schema}.gc_terreno
(t_id
,t_basket
,t_ili_tid
, geometria
, codigo
, etiqueta
, comienzo_vida_util_version
, fin_vida_util_version 
, espacio_de_nombres
, local_id)
SELECT 
 t.id_terreno::bigint,
 (select t_id from {schema}.t_ili2db_basket limit 1),
uuid_generate_v4(),
geometria, 
codigo,
etiqueta,
t.comienzo_vida_util_version::timestamp,
t.fin_vida_util_version::timestamp, 
'gc_terreno' as espacio_de_nombres, 
t.id_terreno as local_id 
FROM tmp_terreno t
WHERE t.geometria IS NOT NULL;