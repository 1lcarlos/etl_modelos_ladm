-- Consulta para extraer datos de cca_estructuranovedadnumeropredial
-- Origen: Modelo CCA (cca_estructuranovedadnumeropredial)
-- Destino: gc_estructuranovedadnumeropredial

SELECT
    enp.t_id,
    enp.t_seq,
    enp.numero_predial,
    -- Mapeo del tipo de novedad al dominio del destino
    tnt.ilicode as tipo_novedad,
    -- Referencia al predio para posteriormente obtener el gc_datosadicionaleslevantamientocatastral
    enp.cca_predio_novedad_numeros_prediales as cca_predio_id,
    p.numero_predial as numero_predial_predio
FROM {schema}.cca_estructuranovedadnumeropredial enp
LEFT JOIN {schema}.cca_estructuranovedadnumeropredial_tipo_novedad tnt ON enp.tipo_novedad = tnt.t_id
LEFT JOIN {schema}.cca_predio p ON enp.cca_predio_novedad_numeros_prediales = p.t_id;
