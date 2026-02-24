-- INSERT para tabla cuc_calificacionconvencional (Modelo Interno Django)
-- Migra calificacion convencional desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_calificacionconvencional (query cca_calificacionconvencional.sql)
-- Destino: cuc_calificacionconvencional (modelo interno Django)
--
-- Diferencias clave con modelo CCA:
--   - CCA tiene estructura PLANA (1 tabla con todos los campos)
--   - Django normaliza en 3 tablas: calificacion, grupo, objeto
--   - PK es 'id' (se usa cca_calificacion_id)
--   - tipo_calificar es FK a cuc_calificartipo (mapeo ilicode -> text_code -> id)
--   - gc_caracteristicasunidadconstruccion es FK directa por CCA id
--
-- Dependencias:
--   - Requiere que gc_caracteristicasunidadconstruccion ya este migrado
--   - Las tablas de dominio (cuc_calificartipo) deben estar pobladas
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_caracteristicasunidadconstruccion

INSERT INTO {schema}.cuc_calificacionconvencional (
    id,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    total_calificacion,
    tipo_calificar,
    gc_caracteristicasunidadconstruccion
)
SELECT
    distinct on (cc.cca_calificacion_id)
    -- id: Usar cca_calificacion_id como id en Django
    --nextval('{schema}.cuc_calificacionconvencional_id_seq'),
    cc.cca_calificacion_id,

    -- espacio_de_nombres
    'CUC_CALIFICACIONCONVENCIONAL_CCA',

    -- local_id
    cc.cca_calificacion_id::varchar,

    -- comienzo_vida_util_version
    NOW(),

    -- fin_vida_util_version
    NULL,

    -- total_calificacion
    cc.total_calificacion,

    -- tipo_calificar: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.cuc_calificartipo
         WHERE text_code = cc.tipo_calificar LIMIT 1),
        (SELECT id FROM {schema}.cuc_calificartipo
         WHERE text_code ILIKE '%' || cc.tipo_calificar || '%' LIMIT 1),
        NULL
    ),

    -- gc_caracteristicasunidadconstruccion: FK directa por CCA id
    cc.cca_caracteristicas_id

FROM tmp_cca_calificacionconvencional cc;
