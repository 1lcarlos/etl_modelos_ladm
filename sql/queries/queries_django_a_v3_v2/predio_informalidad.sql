-- Destino: gc_predio_informalidad
-- 1. REGISTRADA   : relaciones existentes en gc_predio_informalidad de Django.
-- 2. DERIVADA_NPN : predios con condicion Mejora o Informal sin relacion registrada.
--                   Predio formal = mismo NPN en posiciones 1-21 + '000000000'.
--                   Si no existe, id_formal queda NULL y el insert lo registra como incidencia.
-- Las areas del predio informal se usan como areas de interseccion (el informal esta
-- contenido en el formal).
WITH cond AS (
    SELECT p.id, p.numero_predial, p.area, p.area_construida, c.text_code AS condicion
    FROM {schema}.gc_predio AS p
    LEFT JOIN {schema}.gc_condicionprediotipo AS c ON c.id = p.condicion_predio
),
registrada AS (
    SELECT pi.predio_formal AS id_formal, pi.predio_informal AS id_informal, 'REGISTRADA' AS origen_relacion
    FROM {schema}.gc_predio_informalidad AS pi
    WHERE pi.predio_formal IS NOT NULL
      AND pi.predio_informal IS NOT NULL
),
derivada AS (
    SELECT pf.id AS id_formal, ci.id AS id_informal, 'DERIVADA_NPN' AS origen_relacion
    FROM cond AS ci
    LEFT JOIN cond AS pf
           ON pf.numero_predial = left(ci.numero_predial, 21) || '000000000'
          AND pf.id <> ci.id
    WHERE ci.condicion IN ('Mejora', 'Informal')
      AND NOT EXISTS (SELECT 1 FROM registrada AS r WHERE r.id_informal = ci.id)
),
datos AS (
    SELECT
        u.id_formal,
        u.id_informal,
        u.origen_relacion,
        inf.numero_predial   AS npn_informal,
        inf.area             AS area_informal,
        inf.area_construida  AS area_construida_informal
    FROM (SELECT * FROM registrada UNION ALL SELECT * FROM derivada) AS u
    JOIN cond AS inf ON inf.id = u.id_informal
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
