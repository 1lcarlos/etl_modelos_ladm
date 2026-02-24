-- Consulta para extraer relacion terreno-predio del modelo CCA
-- Origen: Modelo CCA (cca_terreno.predio -> cca_predio)
-- Destino: col_uebaunit (relacion gc_terreno -> gc_predio)
-- Fecha: 2026-02-05
--
-- Notas:
-- 1. En CCA la relacion es directa: cca_terreno.predio = cca_predio.t_id
-- 2. En GC se usa tabla intermedia col_uebaunit
-- 3. Se usa numero_predial como clave de relacion para encontrar registros migrados

SELECT
    t.t_id as cca_terreno_id,
    t.predio as cca_predio_id,

    -- Datos para relacionar en destino
    p.numero_predial,
    p.nupre

FROM {schema}.cca_terreno t
INNER JOIN {schema}.cca_predio p ON t.predio = p.t_id
WHERE t.geometria IS NOT NULL
  AND t.predio IS NOT NULL;
