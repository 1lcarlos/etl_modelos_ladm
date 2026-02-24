INSERT INTO {schema}.col_unidadfuente
(t_id, 
t_ili_tid, 
fuente_administrativa, 
unidad)
SELECT
    --id,
    nextval('{schema}.t_ili2db_seq'::regclass),  -- Generar nuevo t_id
    uuid_generate_v4(),     
    fa.t_id,
    p.t_id
FROM
    tmp_col_unidad_fuente tmpunf
    JOIN {schema}.gc_fuenteadministrativa fa ON fa.local_id::numeric = tmpunf.fuente_administrativa::numeric
    JOIN {schema}.gc_predio p on p.local_id::numeric = tmpunf.id_predio::numeric;