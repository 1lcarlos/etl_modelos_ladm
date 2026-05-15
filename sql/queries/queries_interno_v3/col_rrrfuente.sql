-- Consulta para extraer datos de col_rrrfuente del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- En v3 el FK es rrr_gc_derechocatastral, en Django es rrr_gc_derecho
-- Se extraen los local_id para resolver FK en el insert

SELECT
    rr.t_id,
    fa.local_id as fuente_administrativa,
    d.local_id as rrr_gc_derecho,
    rr.rrr_gc_restriccion
FROM {schema}.col_rrrfuente rr
JOIN {schema}.gc_fuenteadministrativa fa ON fa.t_id = rr.fuente_administrativa
LEFT JOIN {schema}.gc_derechocatastral d ON d.t_id = rr.rrr_gc_derechocatastral;
