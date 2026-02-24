-- Consulta para extraer datos de cca_unidadconstruccion para migrar a gc_unidadconstruccion
-- Origen: Modelo CCA (cca_unidadconstruccion)
-- Destino: gc_unidadconstruccion (modelo interno Django)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. cca_unidadconstruccion tiene FK directa a cca_construccion y cca_caracteristicasunidadconstruccion
-- 2. En Django gc_unidadconstruccion NO tiene tipo_planta (se maneja en caracteristicas)
-- 3. El codigo se obtiene del numero_predial del predio via la construccion
-- 4. La geometria debe ser MultiPolygonZ SRID 9377

SELECT
    uc.t_id as cca_unidadconstruccion_id,
    uc.t_ili_tid,
    uc.planta_ubicacion,
    uc.area_construida,
    uc.altura,
    uc.observaciones,
    uc.geometria,

    -- FKs para relaciones en destino
    uc.caracteristicasunidadconstruccion as cca_caracteristicas_id,
    uc.construccion as cca_construccion_id,

    -- Datos de la construccion para codigo y relaciones
    c.identificador as identificador_construccion,
    c.predio as cca_predio_id,

    -- Datos del predio para codigo
    p.numero_predial,

    -- Dominio tipo_planta (para referencia, no existe en gc_unidadconstruccion Django)
    tpt.ilicode as tipo_planta

FROM {schema}.cca_unidadconstruccion uc
INNER JOIN {schema}.cca_construccion c ON uc.construccion = c.t_id
INNER JOIN {schema}.cca_predio p ON c.predio = p.t_id
LEFT JOIN {schema}.cca_construccionplantatipo tpt ON uc.tipo_planta = tpt.t_id;
