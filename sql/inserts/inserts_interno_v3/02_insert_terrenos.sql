INSERT INTO {schema}.gc_terreno
(t_id, t_ili_tid, geometria, codigo, 
etiqueta, comienzo_vida_util_version, fin_vida_util_version, 
espacio_de_nombres, local_id)
SELECT 
 nextval('{schema}.t_ili2db_seq'::regclass),  
    uuid_generate_v4(),
geometria, 
codigo,
etiqueta,
t.comienzo_vida_util_version::timestamp,
t.fin_vida_util_version::timestamp, 
  'gc_terreno' as espacio_de_nombres, 
id 
FROM tmp_terreno t;