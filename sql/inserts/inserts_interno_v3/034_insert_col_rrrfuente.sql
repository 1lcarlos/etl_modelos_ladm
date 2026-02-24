INSERT INTO
    {schema}.col_rrrfuente (
        t_id,
        t_ili_tid,
        fuente_administrativa,        
        rrr_gc_derechocatastral,        
        rrr_gc_restriccion             
    )
SELECT
    --tmprfr.id,
    nextval('{schema}.t_ili2db_seq'::regclass),  -- Generar nuevo t_id
    uuid_generate_v4(),    
    fa.t_id,
    d.t_id,
    tmprfr.rrr_gc_restriccion::numeric
FROM
    tmp_col_rrrfuente tmprfr
    JOIN {schema}.gc_fuenteadministrativa fa ON fa.local_id::numeric = tmprfr.fuente_administrativa::numeric
    JOIN {schema}.gc_derechocatastral d ON d.local_id::numeric = tmprfr.rrr_gc_derecho::numeric;