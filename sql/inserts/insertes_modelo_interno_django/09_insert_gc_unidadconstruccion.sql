-- INSERT para tabla gc_unidadconstruccion (Modelo Interno Django)
-- Migra unidades de construccion desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_unidadconstruccion (query cca_unidadconstruccion.sql)
-- Destino: gc_unidadconstruccion (modelo interno Django)
--
-- Diferencias clave con modelo INTERLIS:
--   - PK es 'id' (se usa cca_unidadconstruccion_id)
--   - No existe t_ili_tid
--   - No existe tipo_planta en gc_unidadconstruccion Django (se maneja en caracteristicas)
--   - No existe relacion_superficie en Django
--   - FKs gc_construccion y gc_caracteristicasunidadconstruccion apuntan a 'id'
--
-- Dependencias:
--   - Requiere que gc_construccion ya este migrado
--   - Requiere que gc_caracteristicasunidadconstruccion ya este migrado
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_construccion y gc_caracteristicasunidadconstruccion

INSERT INTO {schema}.gc_unidadconstruccion (
    id,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    codigo,
    etiqueta,
    area,
    observacion,
    geometria,
    planta_ubicacion,
    altura,
    gc_caracteristicasunidadconstruccion,
    gc_construccion
)
SELECT
    -- id: Usar cca_unidadconstruccion_id como id en Django
    uc.cca_unidadconstruccion_id,

    -- espacio_de_nombres
    'GC_UNIDADCONSTRUCCION_CCA',

    -- local_id
    uc.cca_construccion_id::varchar,

    -- comienzo_vida_util_version
    NOW(),

    -- fin_vida_util_version
    NULL,

    -- codigo: Usar numero_predial del predio
    uc.numero_predial,

    -- etiqueta
    uc.cca_caracteristicas_id, 

    -- area
    COALESCE(uc.area_construida::numeric(16,2), 0),

    -- observacion
    uc.observaciones,

    -- geometria: Asegurar MultiPolygonZ SRID 9377
    CASE
        WHEN uc.geometria IS NOT NULL AND ST_GeometryType(uc.geometria) = 'ST_MultiPolygon' THEN
            ST_Force3D(uc.geometria)
        WHEN uc.geometria IS NOT NULL AND ST_GeometryType(uc.geometria) = 'ST_Polygon' THEN
            ST_Force3D(ST_Multi(uc.geometria))
        WHEN uc.geometria IS NOT NULL THEN
            ST_Force3D(ST_Multi(ST_MakeValid(uc.geometria)))
        ELSE NULL
    END,

    -- planta_ubicacion
    uc.planta_ubicacion,

    -- altura
    uc.altura::numeric(6,2),

    -- gc_caracteristicasunidadconstruccion: FK directa por CCA id
    (SELECT gcu.id FROM {schema}.gc_caracteristicasunidadconstruccion gcu
     WHERE gcu.id = uc.cca_caracteristicas_id
     LIMIT 1),

    -- gc_construccion: FK directa por CCA id
    (SELECT gc.id FROM {schema}.gc_construccion gc
     WHERE gc.id = uc.cca_construccion_id
     LIMIT 1)

FROM tmp_cca_unidadconstruccion uc
WHERE EXISTS (
    -- Solo insertar si la construccion ya fue migrada
    SELECT 1 FROM {schema}.gc_construccion gc
    WHERE gc.id = uc.cca_construccion_id
)
AND EXISTS (
    -- Solo insertar si las caracteristicas ya fueron migradas
    SELECT 1 FROM {schema}.gc_caracteristicasunidadconstruccion gcu
    WHERE gcu.id = uc.cca_caracteristicas_id
);
