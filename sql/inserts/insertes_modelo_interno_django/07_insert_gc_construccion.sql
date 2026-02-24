-- INSERT para tabla gc_construccion (Modelo Interno Django)
-- Migra construcciones desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_construccion (query cca_construccion.sql)
-- Destino: gc_construccion (modelo interno Django)
--
-- Diferencias clave con modelo INTERLIS:
--   - PK es 'id' (se usa cca_construccion_id)
--   - No existe t_ili_tid
--   - Dominios usan text_code (no ilicode) y FK apunta a 'id' (no t_id)
--   - No existe FK directa a predio (la relacion va por col_uebaunit)
--
-- Dependencias:
--   - Requiere que gc_predio ya este migrado
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_predio

INSERT INTO {schema}.gc_construccion (
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
    identificador,
    numero_pisos,
    numero_sotanos,
    numero_mezanines,
    numero_semisotanos,
    anio_construccion,
    altura,
    valor_referencia,
    tipo_construccion,
    tipo_dominio
)
SELECT
    -- id: Usar cca_construccion_id como id en Django
    c.cca_construccion_id,

    -- espacio_de_nombres
    'GC_CONSTRUCCION_CCA',

    -- local_id: Usar cca_predio_id para relacion
    c.cca_predio_id::varchar,

    -- comienzo_vida_util_version
    NOW(),

    -- fin_vida_util_version
    NULL,

    -- codigo: Usar numero_predial del predio relacionado
    c.numero_predial,

    -- etiqueta
    COALESCE(c.etiqueta, c.numero_predial),

    -- area
    COALESCE(c.area_construccion_digital::numeric(16,2), 0),

    -- observacion
    c.observaciones,

    -- geometria: Asegurar MultiPolygonZ SRID 9377
    CASE
        WHEN c.geometria IS NOT NULL AND ST_GeometryType(c.geometria) = 'ST_MultiPolygon' THEN
            ST_Force3D(c.geometria)
        WHEN c.geometria IS NOT NULL AND ST_GeometryType(c.geometria) = 'ST_Polygon' THEN
            ST_Force3D(ST_Multi(c.geometria))
        WHEN c.geometria IS NOT NULL THEN
            ST_Force3D(ST_Multi(ST_MakeValid(c.geometria)))
        ELSE NULL
    END,

    -- identificador
    c.identificador,

    -- numero_pisos
    c.numero_pisos::numeric,

    -- numero_sotanos
    c.numero_sotanos::numeric,

    -- numero_mezanines
    c.numero_mezanines::numeric,

    -- numero_semisotanos
    c.numero_semisotanos::numeric,

    -- anio_construccion
    c.anio_construccion::numeric,

    -- altura
    c.altura::numeric(6,2),

    -- valor_referencia
    c.valor_referencia_construccion::numeric,

    -- tipo_construccion: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_construcciontipo
         WHERE text_code = c.tipo_construccion LIMIT 1),
        (SELECT id FROM {schema}.gc_construcciontipo
         WHERE text_code ILIKE '%' || c.tipo_construccion || '%' LIMIT 1),
        NULL
    ),

    -- tipo_dominio: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_dominioconstrucciontipo
         WHERE text_code = c.tipo_dominio LIMIT 1),
        (SELECT id FROM {schema}.gc_dominioconstrucciontipo
         WHERE text_code ILIKE '%' || c.tipo_dominio || '%' LIMIT 1),
        NULL
    )

FROM tmp_cca_construccion c
WHERE EXISTS (
    -- Solo insertar si el predio ya fue migrado
    SELECT 1 FROM {schema}.gc_predio gp
    WHERE gp.id = c.cca_predio_id
);
