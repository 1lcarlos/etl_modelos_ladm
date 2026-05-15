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
FROM {schema}.gc_terreno t;
