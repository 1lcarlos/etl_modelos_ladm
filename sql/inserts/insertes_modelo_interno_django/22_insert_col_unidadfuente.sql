-- INSERT para tabla col_unidadfuente (Modelo Interno Django)
-- Migra relacion fuente administrativa-predio desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_unidadfuente (query cca_unidadfuente.sql)
-- Destino: col_unidadfuente (modelo interno Django)
--
-- Diferencias clave con modelo CCA:
--   - PK es 'id' auto-generado con secuencia
--   - En CCA la relacion es indirecta: fuente -> derecho -> predio
--   - En Django es directa: fuente_administrativa -> unidad (predio)
--   - La query ya resuelve el JOIN y entrega pares (fuente, predio) con DISTINCT
--
-- Dependencias:
--   - Requiere que gc_predio ya este migrado (FASE 1)
--   - Requiere que gc_fuenteadministrativa ya este migrado (FASE 20)
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_predio y gc_fuenteadministrativa

INSERT INTO {schema}.col_unidadfuente (
    id,
    unidad,
    fuente_administrativa
)
SELECT
    -- id: Secuencia auto-generada
    nextval('{schema}.col_unidadfuente_id_seq'),

    -- unidad: FK a gc_predio por cca_predio_id
    (SELECT p.id FROM {schema}.gc_predio p
     WHERE p.id = uf.cca_predio_id::numeric
     LIMIT 1),

    -- fuente_administrativa: FK a gc_fuenteadministrativa por cca_fuenteadministrativa_id
    (SELECT fa.id FROM {schema}.gc_fuenteadministrativa fa
     WHERE fa.id = uf.cca_fuenteadministrativa_id::numeric
     LIMIT 1)

FROM tmp_cca_unidadfuente uf
WHERE EXISTS (
    SELECT 1 FROM {schema}.gc_predio p
    WHERE p.id = uf.cca_predio_id::numeric
)
AND EXISTS (
    SELECT 1 FROM {schema}.gc_fuenteadministrativa fa
    WHERE fa.id = uf.cca_fuenteadministrativa_id::numeric
);
