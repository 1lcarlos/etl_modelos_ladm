-- INSERT para tabla gc_terreno
-- Migra terrenos desde modelo CCA a modelo GC
-- Fecha: 2026-02-05
--
-- Dependencias:
--   - Usa datos de tmp_cca_terreno (query cca_terreno.sql)
--   - Requiere que gc_predio ya este migrado
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_predio

INSERT INTO {schema}.gc_terreno (
    t_id,
    t_ili_tid,
    geometria,
    codigo,
    etiqueta,
    relacion_superficie,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),

    -- geometria (NOT NULL): Asegurar MultiPolygonZ SRID 9377
    CASE
        WHEN ST_GeometryType(t.geometria) = 'ST_MultiPolygon' THEN
            ST_Force3D(t.geometria)
        WHEN ST_GeometryType(t.geometria) = 'ST_Polygon' THEN
            ST_Force3D(ST_Multi(t.geometria))
        ELSE
            ST_Force3D(ST_Multi(ST_MakeValid(t.geometria)))
    END,

    -- codigo: Usar numero_predial del predio relacionado
    t.numero_predial,

    -- etiqueta
    COALESCE(t.etiqueta, t.numero_predial),

    -- relacion_superficie: Mapeo a col_relacionsuperficietipo (En_Rasante por defecto)
    (SELECT t_id FROM {schema}.col_relacionsuperficietipo
     WHERE ilicode = 'En_Rasante' LIMIT 1),

    -- comienzo_vida_util_version (NOT NULL)
    NOW(),

    -- fin_vida_util_version
    NULL,

    -- espacio_de_nombres (NOT NULL)
    'GC_TERRENO_CCA',

    -- local_id (NOT NULL): Usar t_id original de CCA
    t.cca_terreno_id::varchar

FROM tmp_cca_terreno t
WHERE t.geometria IS NOT NULL
  AND EXISTS (
      -- Solo insertar si el predio ya fue migrado
      SELECT 1 FROM {schema}.gc_predio gp
      WHERE gp.numero_predial = t.numero_predial
  );
