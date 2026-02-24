-- INSERT para tabla gc_terreno (Modelo Interno Django)
-- Migra terrenos desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_terreno (query cca_terreno.sql)
-- Destino: gc_terreno (modelo interno Django)
--
-- Diferencias clave con modelo INTERLIS:
--   - PK es 'id' auto-generado (no t_id)
--   - No existe t_ili_tid
--   - No existe relacion_superficie en Django
--
-- Dependencias:
--   - Requiere que gc_predio ya este migrado
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_predio

INSERT INTO {schema}.gc_terreno (
    id, 
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    codigo,
    etiqueta,
    area,
    observacion,
    geometria
)
SELECT
    t.cca_terreno_id,

    -- espacio_de_nombres
    'GC_TERRENO_CCA',

    -- local_id: Usar cca_terreno_id como identificador para relaciones posteriores
    t.cca_predio_id::varchar,

    -- comienzo_vida_util_version
    NOW(),

    -- fin_vida_util_version
    NULL,

    -- codigo: Usar numero_predial del predio relacionado
    t.numero_predial,

    -- etiqueta
    COALESCE(t.etiqueta, t.numero_predial),

    -- area
    COALESCE(t.area_terreno::numeric(16,2), 0),

    -- observacion
    NULL,

    -- geometria: Asegurar MultiPolygonZ SRID 9377
    CASE
        WHEN ST_GeometryType(t.geometria) = 'ST_MultiPolygon' THEN
            ST_Force3D(t.geometria)
        WHEN ST_GeometryType(t.geometria) = 'ST_Polygon' THEN
            ST_Force3D(ST_Multi(t.geometria))
        ELSE
            ST_Force3D(ST_Multi(ST_MakeValid(t.geometria)))
    END

FROM tmp_cca_terreno t
WHERE t.geometria IS NOT NULL
  AND EXISTS (
      -- Solo insertar si el predio ya fue migrado
      SELECT 1 FROM {schema}.gc_predio gp
      WHERE gp.id = t.cca_predio_id::numeric
  );
