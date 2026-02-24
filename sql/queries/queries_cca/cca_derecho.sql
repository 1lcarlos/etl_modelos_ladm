-- Consulta para extraer datos de cca_derecho
-- Origen: Modelo CCA (cca_derecho + dominios)
-- Destino: gc_derecho (modelo Django)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. Los dominios se extraen con ilicode para mapeo con text_code en destino
-- 2. CCA fraccion_derecho rango 0-100, Django rango 0-1 (dividir entre 100)
-- 3. CCA tiene cuota_participacion y origen_derecho que no existen en Django
-- 4. CCA observacion -> Django descripcion
-- 5. CCA predio -> Django baunit
-- 6. CCA agrupacion_interesados -> Django interesado_gc_agrupacioninteresados
-- 7. CCA interesado -> Django interesado_gc_interesado

SELECT
    d.t_id as cca_derecho_id,
    d.t_ili_tid,
    d.fraccion_derecho,
    d.fecha_inicio_tenencia,
    d.observacion,

    -- FKs directas
    d.predio as cca_predio_id,
    d.interesado as cca_interesado_id,
    d.agrupacion_interesados as cca_agrupacion_interesados_id,

    -- Dominios con ilicode para mapeo en destino
    tdt.ilicode as tipo,
    odt.ilicode as origen_derecho

FROM {schema}.cca_derecho d
LEFT JOIN {schema}.cca_derechotipo tdt ON d.tipo = tdt.t_id
LEFT JOIN {schema}.cca_origenderechotipo odt ON d.origen_derecho = odt.t_id;
