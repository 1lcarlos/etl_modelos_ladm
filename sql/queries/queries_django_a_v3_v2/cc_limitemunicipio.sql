-- Destino: cc_limitemunicipio (basket LimitesOperativosCatastro)
-- codigo_municipio puede venir con 3 o 5 digitos; el insert toma los 3 ultimos.
WITH datos AS (
    SELECT c.id, c.codigo_departamento, c.codigo_municipio, c.nombre_municipio, c.geometria
    FROM {schema}.cc_limitemunicipio AS c
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
