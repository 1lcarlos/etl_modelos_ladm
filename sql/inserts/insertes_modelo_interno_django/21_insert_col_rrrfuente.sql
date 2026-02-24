-- INSERT para tabla col_rrrfuente (Modelo Interno Django)
-- Migra relacion fuente administrativa-derecho desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_fuenteadministrativa_derecho (query cca_fuenteadministrativa_derecho.sql)
-- Destino: col_rrrfuente (modelo interno Django)
--
-- Diferencias clave con modelo CCA:
--   - PK es 'id' auto-generado con secuencia
--   - CCA tiene tabla separada cca_fuenteadministrativa_derecho
--   - Django col_rrrfuente tiene rrr_gc_restriccion (NULL para relacion derecho-fuente)
--
-- Dependencias:
--   - Requiere que gc_derecho ya este migrado (FASE 19)
--   - Requiere que gc_fuenteadministrativa ya este migrado (FASE 20)
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_derecho y gc_fuenteadministrativa

INSERT INTO {schema}.col_rrrfuente (
    id,
    fuente_administrativa,
    rrr_gc_derecho,
    rrr_gc_restriccion
)
SELECT
    -- id: Secuencia auto-generada
    nextval('{schema}.col_rrrfuente_id_seq'),

    -- fuente_administrativa: FK a gc_fuenteadministrativa por cca_fuenteadministrativa_id
    (SELECT fa.id FROM {schema}.gc_fuenteadministrativa fa
     WHERE fa.id = fd.cca_fuenteadministrativa_id
     LIMIT 1),

    -- rrr_gc_derecho: FK a gc_derecho por cca_derecho_id
    (SELECT der.id FROM {schema}.gc_derecho der
     WHERE der.id = fd.cca_derecho_id
     LIMIT 1),

    -- rrr_gc_restriccion: No aplica para relacion derecho-fuente
    NULL

FROM tmp_cca_fuenteadministrativa_derecho fd
WHERE EXISTS (
    SELECT 1 FROM {schema}.gc_derecho der
    WHERE der.id = fd.cca_derecho_id
)
AND EXISTS (
    SELECT 1 FROM {schema}.gc_fuenteadministrativa fa
    WHERE fa.id = fd.cca_fuenteadministrativa_id
);
