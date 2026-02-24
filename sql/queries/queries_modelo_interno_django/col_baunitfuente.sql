-- Query para extraer relacion baunit-fuente
-- Fecha: 2025-12-18

SELECT
    bf.id,
    bf.fuente,
    bf.unidad
FROM {schema}.col_baunitfuente bf
WHERE bf.fuente IS NOT NULL
  AND bf.unidad IS NOT NULL;
