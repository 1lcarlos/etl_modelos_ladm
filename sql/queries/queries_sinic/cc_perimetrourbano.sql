-- Consulta para extraer datos de la tabla cc_perimetrourbano

SELECT
    id,
    geometria,
    codigo_departamento,
    codigo_municipio,
    tipo_avaluo,
    nombre_geografico,
    codigo_nombre
FROM {schema}.cc_perimetrourbano;
