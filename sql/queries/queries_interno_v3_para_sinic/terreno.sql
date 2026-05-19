-- Consulta para extraer datos de gc_terreno del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django

SELECT
    COALESCE(t.espacio_de_nombres, 'gc_terreno') as espacio_de_nombres,
    t.local_id,
    t.t_id,
    COALESCE(t.comienzo_vida_util_version::timestamp with time zone, now()) as comienzo_vida_util_version,
    t.fin_vida_util_version::timestamp with time zone as fin_vida_util_version,
    t.codigo,
    COALESCE(t.etiqueta, '0') as etiqueta,
    t.geometria
FROM {schema}.gc_terreno t
join {schema}.col_uebaunit ue on t.t_id = ue.ue_gc_terreno
join {schema}.gc_predio p on p.t_id = ue.baunit
JOIN {schema}.gc_predio_tramitecatastral ptc ON p.t_id = ptc.cr_predio;
    