-- Procedimientos catastrales-registrales por predio
-- Fuente: resolucion de Django (vacía en la mayoría de municipios)
-- Nota: resolucion no tiene FK directa a gc_predio; se aproxima por municipio_predio.
SELECT
    r.tipo          AS tipo_procedimiento,
    p.local_id      AS predio_local_id
FROM {schema}.resolucion r
JOIN {schema}.gc_predio p
    ON LEFT(p.numero_predial, 5) = r.municipio_predio
WHERE r.generado = TRUE;
