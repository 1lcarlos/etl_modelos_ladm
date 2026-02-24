-- Consulta para extraer datos de cca_terreno para migrar a gc_terreno
-- Origen: Modelo CCA (cca_terreno)
-- Destino: gc_terreno
-- Fecha: 2026-02-05
--
-- Notas:
-- 1. cca_terreno tiene relacion directa con cca_predio via campo 'predio'
-- 2. El codigo se genera a partir del numero_predial del predio relacionado
-- 3. La geometria debe ser MultiPolygonZ SRID 9377

SELECT
    t.t_id as cca_terreno_id,
    t.t_ili_tid,
    t.geometria,
    t.area_terreno,
    t.etiqueta,
    t.servidumbre_transito,

    -- Relacion con predio (para col_uebaunit)
    t.predio as cca_predio_id,

    -- Obtener numero_predial del predio para usar como codigo
    p.numero_predial,
    p.nupre

FROM {schema}.cca_terreno t
INNER JOIN {schema}.cca_predio p ON t.predio = p.t_id
WHERE t.geometria IS NOT NULL;
