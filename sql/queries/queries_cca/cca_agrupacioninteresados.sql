-- Consulta para extraer datos de cca_agrupacioninteresados
-- Origen: Modelo CCA (cca_agrupacioninteresados + dominios)
-- Destino: gc_agrupacioninteresados (modelo Django)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. El dominio tipo se extrae con ilicode para mapeo con text_code en destino
-- 2. CCA referencia cca_grupointeresadotipo, Django referencia col_grupointeresadotipo

SELECT
    a.t_id as cca_agrupacion_id,
    a.t_ili_tid,
    a.nombre,

    -- Dominio tipo con ilicode
    git.ilicode as tipo

FROM {schema}.cca_agrupacioninteresados a
LEFT JOIN {schema}.cca_grupointeresadotipo git ON a.tipo = git.t_id;
