INSERT INTO {schema}.col_areavalor
( t_basket
, t_ili_tid
, tipo
, area
, cr_terreno_area
, datos_proyeccion)
select 
(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1)
,uuid_generate_v4()
,(select t_id from {schema}.col_areatipo at where at.ilicode = p.tipo_area)
, p.area_catastral_terreno::numeric(15,1)
, p.id_terreno
, p.datos_proyeccion
from tmp_col_areavalor_terreno p;