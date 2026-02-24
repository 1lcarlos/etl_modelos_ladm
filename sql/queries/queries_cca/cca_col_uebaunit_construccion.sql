-- Consulta para extraer relacion construccion-predio del modelo CCA
-- Origen: Modelo CCA (cca_construccion.predio -> cca_predio)
-- Destino: col_uebaunit (relacion gc_construccion -> gc_predio)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. En CCA la relacion es directa: cca_construccion.predio = cca_predio.t_id
-- 2. En Django se usa tabla intermedia col_uebaunit
-- 3. Se usa cca_predio_id y cca_construccion_id como claves de relacion

SELECT
    c.t_id as cca_construccion_id,
    c.predio as cca_predio_id,

    -- Datos para referencia
    p.numero_predial,
    p.nupre

FROM {schema}.cca_construccion c
INNER JOIN {schema}.cca_predio p ON c.predio = p.t_id;
