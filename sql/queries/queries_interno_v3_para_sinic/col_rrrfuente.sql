-- Consulta para extraer datos de col_rrrfuente del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- En v3 el FK es rrr_gc_derechocatastral, en Django es rrr_gc_derecho
-- Se extraen los local_id para resolver FK en el insert

SELECT
    rr.t_id,
    fa.t_id as id_fuente_administrativa,
    d.t_id as id_derecho
    FROM {schema}.col_rrrfuente rr
JOIN {schema}.gc_fuenteadministrativa fa ON fa.t_id = rr.fuente_administrativa
 JOIN {schema}.gc_derechocatastral d ON d.t_id = rr.rrr_gc_derechocatastral
 JOIN {schema}.gc_predio p ON p.t_id = d.unidad
JOIN {schema}.gc_predio_tramitecatastral ptc ON p.t_id = ptc.cr_predio;