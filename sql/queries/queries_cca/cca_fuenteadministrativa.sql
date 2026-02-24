-- Consulta para extraer datos de cca_fuenteadministrativa
-- Origen: Modelo CCA (cca_fuenteadministrativa + dominios)
-- Destino: gc_fuenteadministrativa (modelo Django)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. El dominio tipo se extrae con ilicode para mapeo con text_code en destino
-- 2. CCA observacion -> Django descripcion
-- 3. Django tiene valor_transaccion y estado_disponibilidad que no existen en CCA

SELECT
    f.t_id as cca_fuenteadministrativa_id,
    f.t_ili_tid,
    f.numero_fuente,
    f.fecha_documento_fuente,
    f.ente_emisor,
    f.observacion,

    -- Dominio tipo con ilicode
    fat.ilicode as tipo

FROM {schema}.cca_fuenteadministrativa f
LEFT JOIN {schema}.cca_fuenteadministrativatipo fat ON f.tipo = fat.t_id;
