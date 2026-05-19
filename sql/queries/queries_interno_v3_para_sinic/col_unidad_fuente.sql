-- Consulta para extraer datos de col_unidadfuente del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- Se extraen los local_id de predio y fuente administrativa para resolver FK en el insert

SELECT
    cu.t_id,
    p.t_id as predio_id,
    fa.t_id as fuente_administrativa_id
FROM {schema}.col_unidadfuente cu
JOIN {schema}.gc_predio p ON cu.unidad = p.t_id
JOIN {schema}.gc_fuenteadministrativa fa ON cu.fuente_administrativa = fa.t_id
JOIN {schema}.gc_predio_tramitecatastral ptc ON p.t_id = ptc.cr_predio;
