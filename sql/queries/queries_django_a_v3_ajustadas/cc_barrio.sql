-- Consulta para extraer datos de la tabla cc_barrio

SELECT
    id,
    geometria,
    codigo,
    nombre,
    codigo_sector
FROM {schema}.cc_barrio;
