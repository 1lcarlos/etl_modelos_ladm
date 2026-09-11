-- Consulta para extraer datos de col_miembros del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- Se extraen los local_id de interesado y agrupación para resolver FK en el insert

SELECT
    cm.t_id,
    cm.participacion,
    i.t_id as interesado_id,
    ai.t_id as agrupacion_id
FROM {schema}.col_miembros cm
JOIN {schema}.gc_interesado i ON cm.interesado_gc_interesado = i.t_id
JOIN {schema}.gc_agrupacioninteresados ai ON cm.agrupacion = ai.t_id
join {schema}.gc_derechocatastral dc on ai.t_id = dc.interesado_gc_agrupacioninteresados 
join {schema}.gc_predio p on dc.unidad = p.t_id 
JOIN {schema}.gc_predio_tramitecatastral ptc ON p.t_id = ptc.cr_predio;
