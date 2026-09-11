-- Consulta para extraer datos de la tabla cc_limitemunicipio

SELECT
    id,
    geometria,
    codigo_departamento,
    codigo_municipio,
    nombre_municipio
FROM {schema}.cc_limitemunicipio;
