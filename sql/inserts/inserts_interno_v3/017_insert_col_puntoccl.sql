-- Insert para la tabla col_puntoccl
-- Esta tabla relaciona los puntos (control, lindero) con los linderos (ccl)
INSERT INTO {schema}.col_puntoccl (
    t_id,
    ccl,
    punto_gc_puntocontrol,
    punto_gc_puntolindero
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    l.t_id,
    pc.t_id,
    pli.t_id
FROM tmp_col_puntoccl tp
JOIN {schema}.gc_lindero l ON tp.ccl::text = l.local_id
LEFT JOIN {schema}.gc_puntocontrol pc ON tp.punto_gc_puntocontrol::text = pc.local_id
LEFT JOIN {schema}.gc_puntolindero pli ON tp.punto_gc_puntolindero::text = pli.local_id;
