-- Destino: cc_perimetrourbano (basket LimitesOperativosCatastro)
-- tipo_avaluo (01 cabecera, 02.. corregimientos) usa la misma codificacion de zona del NPN.
WITH datos AS (
    SELECT
        c.id,
        c.codigo_departamento,
        c.codigo_municipio,
        c.tipo_avaluo,
        c.nombre_geografico,
        c.codigo_nombre,
        c.geometria
    FROM {schema}.cc_perimetrourbano AS c
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
