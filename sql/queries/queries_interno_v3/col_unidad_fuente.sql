-- Consulta para extraer datos de col_unidadfuente del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- Se extraen los local_id de predio y fuente administrativa para resolver FK en el insert

SELECT
    cu.t_id,
    p.local_id as id_predio,
    fa.local_id as fuente_administrativa
FROM {schema}.col_unidadfuente cu
JOIN {schema}.gc_predio p ON cu.unidad = p.t_id
JOIN {schema}.gc_fuenteadministrativa fa ON cu.fuente_administrativa = fa.t_id;
