-- Consulta para extraer relación predio-terreno de col_uebaunit del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- Se extraen los local_id para hacer join en el insert contra las tablas Django

SELECT
    cu.t_id,
    t.local_id as ue_gc_terreno,
    p.local_id as unidad
FROM {schema}.col_uebaunit cu
JOIN {schema}.gc_terreno t ON cu.ue_gc_terreno = t.t_id
JOIN {schema}.gc_predio p ON cu.baunit = p.t_id;
