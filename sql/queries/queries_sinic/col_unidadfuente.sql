-- Query para extraer datos de col_unidadfuente para migrar
-- Origen: {schema}.col_unidadfuente
-- Destino: sinic2.col_unidadfuente
-- Fecha: 2026-02-02

SELECT DISTINCT ON (uf.id)
    uf.id,
    uf.fuente_administrativa as fuente_administrativa_id,
    uf.unidad as predio_id
FROM {schema}.col_unidadfuente uf
ORDER BY uf.id;
