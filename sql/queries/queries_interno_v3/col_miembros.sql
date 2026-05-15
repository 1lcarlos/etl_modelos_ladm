-- Consulta para extraer datos de col_miembros del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- Se extraen los local_id de interesado y agrupación para resolver FK en el insert

SELECT
    cm.t_id,
    cm.participacion,
    i.local_id as interesado_local_id,
    ai.local_id as agrupacion_local_id
FROM {schema}.col_miembros cm
JOIN {schema}.gc_interesado i ON cm.interesado_gc_interesado = i.t_id
JOIN {schema}.gc_agrupacioninteresados ai ON cm.agrupacion = ai.t_id;
