-- Destino: col_uebaunit (terreno -> predio)
-- Enlace en cascada:
--   1. DIRECTO     : col_uebaunit.ue_gc_terreno
--   2. CODIGO_NPN  : gc_terreno.codigo = gc_predio.numero_predial (solo terrenos sin enlace directo)
WITH directo AS (
    SELECT DISTINCT ue.ue_gc_terreno AS id_terreno, ue.unidad AS id_predio
    FROM {schema}.col_uebaunit AS ue
    WHERE ue.ue_gc_terreno IS NOT NULL
      AND ue.unidad IS NOT NULL
),
por_npn AS (
    SELECT t.id AS id_terreno, p.id AS id_predio
    FROM {schema}.gc_terreno AS t
    JOIN {schema}.gc_predio AS p ON p.numero_predial = t.codigo
    WHERE NOT EXISTS (SELECT 1 FROM directo AS d WHERE d.id_terreno = t.id)
),
datos AS (
    SELECT id_terreno, id_predio, 'DIRECTO' AS metodo_enlace FROM directo
    UNION ALL
    SELECT id_terreno, id_predio, 'CODIGO_NPN' FROM por_npn
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
