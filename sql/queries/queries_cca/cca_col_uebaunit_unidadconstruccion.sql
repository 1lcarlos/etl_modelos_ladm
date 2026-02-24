-- Consulta para extraer relacion unidadconstruccion-predio del modelo CCA
-- Origen: Modelo CCA (cca_unidadconstruccion -> cca_construccion -> cca_predio)
-- Destino: col_uebaunit (relacion gc_unidadconstruccion -> gc_predio)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. En CCA la relacion es indirecta: unidadconstruccion -> construccion -> predio
-- 2. En Django se usa tabla intermedia col_uebaunit
-- 3. Se usa cca_predio_id y cca_unidadconstruccion_id como claves de relacion

SELECT
    uc.t_id as cca_unidadconstruccion_id,
    uc.construccion as cca_construccion_id,
    c.predio as cca_predio_id,

    -- Datos para referencia
    p.numero_predial,
    p.nupre

FROM {schema}.cca_unidadconstruccion uc
INNER JOIN {schema}.cca_construccion c ON uc.construccion = c.t_id
INNER JOIN {schema}.cca_predio p ON c.predio = p.t_id;
