-- Destino: col_uebaunit (unidad de construccion -> predio)
-- Enlace en cascada (se usa el primer nivel que exista para cada unidad):
--   1. DIRECTO          : col_uebaunit.ue_gc_unidadconstruccion
--   2. VIA_CONSTRUCCION : unidad -> gc_construccion -> col_uebaunit.ue_gc_construccion
--   3. NPN_LOCAL_ID     : gc_unidadconstruccion.local_id = gc_predio.numero_predial
--   4. SIN_ENLACE       : la unidad se migra, pero sin predio (queda como incidencia)
-- id_predio_npn permite detectar unidades cuyo NPN contradice el enlace formal.
WITH uc AS (
    SELECT u.id, u.gc_construccion, u.local_id
    FROM {schema}.gc_unidadconstruccion AS u
    WHERE u.geometria IS NOT NULL
),
directo AS (
    SELECT DISTINCT ue.ue_gc_unidadconstruccion AS id_uc, ue.unidad AS id_predio
    FROM {schema}.col_uebaunit AS ue
    JOIN uc ON uc.id = ue.ue_gc_unidadconstruccion
    WHERE ue.unidad IS NOT NULL
),
via_construccion AS (
    SELECT DISTINCT uc.id AS id_uc, ue.unidad AS id_predio
    FROM uc
    JOIN {schema}.col_uebaunit AS ue ON ue.ue_gc_construccion = uc.gc_construccion
    WHERE ue.unidad IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM directo AS d WHERE d.id_uc = uc.id)
),
por_npn AS (
    SELECT uc.id AS id_uc, p.id AS id_predio
    FROM uc
    JOIN {schema}.gc_predio AS p ON p.numero_predial = uc.local_id
    WHERE NOT EXISTS (SELECT 1 FROM directo AS d WHERE d.id_uc = uc.id)
      AND NOT EXISTS (SELECT 1 FROM via_construccion AS v WHERE v.id_uc = uc.id)
),
enlaces AS (
    SELECT id_uc, id_predio, 'DIRECTO' AS metodo_enlace FROM directo
    UNION ALL
    SELECT id_uc, id_predio, 'VIA_CONSTRUCCION' FROM via_construccion
    UNION ALL
    SELECT id_uc, id_predio, 'NPN_LOCAL_ID' FROM por_npn
),
todos AS (
    SELECT id_uc, id_predio, metodo_enlace FROM enlaces
    UNION ALL
    SELECT uc.id, NULL::bigint, 'SIN_ENLACE'
    FROM uc
    WHERE NOT EXISTS (SELECT 1 FROM enlaces AS e WHERE e.id_uc = uc.id)
),
datos AS (
    SELECT
        t.id_uc,
        t.id_predio,
        t.metodo_enlace,
        pn.id AS id_predio_npn,
        (pn.id IS NOT NULL
         AND NOT EXISTS (SELECT 1 FROM todos AS t2 WHERE t2.id_uc = t.id_uc AND t2.id_predio = pn.id)
        ) AS contradice_npn
    FROM todos AS t
    JOIN uc ON uc.id = t.id_uc
    LEFT JOIN {schema}.gc_predio AS pn ON pn.numero_predial = uc.local_id
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
