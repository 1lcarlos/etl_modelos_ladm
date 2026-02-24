-- Consulta para extraer datos de gc_estructuranovedadnumeropredial
-- Origen: Modelo interno django (gc_estructuranovedadnumeropredial)
-- Destino:Modelo interno v3 gc_estructuranovedadnumeropredial

SELECT
    enp.id,
    --enp.t_seq,
    enp.numero_predial,
    -- Mapeo del tipo de novedad al dominio del destino
    tnt.ilicode as tipo_novedad,
    -- Referencia al predio para posteriormente obtener el gc_datosadicionaleslevantamientocatastral
    enp.gc_predio_novedad_numeros_prediales as gc_predio_id,
    p.numero_predial as numero_predial_predio
FROM {schema}.gc_estructuranovedadnumeropredial enp
LEFT JOIN {schema}.gc_estructuranovedadnumeropredial_tipo_novedad tnt ON enp.tipo_novedad = tnt.id
left join {schema}.dlc_datosadicionaleslevantamientocatastral dd on enp.gc_predio_novedad_numeros_prediales = dd.id
LEFT JOIN {schema}.gc_predio p ON dd.gc_predio = p.id;