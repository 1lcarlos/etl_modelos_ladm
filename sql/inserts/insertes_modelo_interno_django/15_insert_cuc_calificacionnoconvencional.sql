-- INSERT para tabla cuc_calificacionnoconvencional (Modelo Interno Django)
-- Migra calificacion no convencional desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_caracteristicasunidadconstruccion (query cca_caracteristicasunidadconstruccion.sql)
-- Destino: cuc_calificacionnoconvencional (modelo interno Django)
--
-- En CCA no existe tabla cca_calificacionnoconvencional.
-- El campo tipo_anexo esta en cca_caracteristicasunidadconstruccion directamente.
-- En Django se normaliza en tabla separada cuc_calificacionnoconvencional.
--
-- Mapeo:
--   - tipo_anexo: ilicode de cca_anexotipo -> text_code de cuc_anexotipo -> id
--   - gc_caracteristicasunidadconstruccion: FK directa por CCA id
--
-- Solo se insertan registros donde tipo_anexo IS NOT NULL
--
-- Dependencias:
--   - Requiere que gc_caracteristicasunidadconstruccion ya este migrado (FASE 8)
--   - Las tablas de dominio (cuc_anexotipo) deben estar pobladas
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_caracteristicasunidadconstruccion

INSERT INTO {schema}.cuc_calificacionnoconvencional (
    id,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    tipo_anexo,
    gc_caracteristicasunidadconstruccion
)
SELECT
    -- id: Secuencia auto-generada
    nextval('{schema}.cuc_calificacionnoconvencional_id_seq'),

    -- espacio_de_nombres
    'CUC_CALIFICACIONNOCONVENCIONAL_CCA',

    -- local_id: Referencia al CCA id de caracteristicas
    cu.cca_caracteristicas_id::varchar,

    -- comienzo_vida_util_version
    NOW(),

    -- fin_vida_util_version
    NULL,

    -- tipo_anexo: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.cuc_anexotipo
         WHERE text_code = cu.tipo_anexo LIMIT 1),
        (SELECT id FROM {schema}.cuc_anexotipo
         WHERE text_code ILIKE '%' || cu.tipo_anexo || '%' LIMIT 1),
        NULL
    ),

    -- gc_caracteristicasunidadconstruccion: FK directa por CCA id
    cu.cca_caracteristicas_id

FROM tmp_cca_caracteristicasunidadconstruccion cu
WHERE cu.tipo_anexo IS NOT NULL
AND EXISTS (
    SELECT 1 FROM {schema}.gc_caracteristicasunidadconstruccion gcu
    WHERE gcu.id = cu.cca_caracteristicas_id
);
