-- INSERT para tabla col_miembros (Modelo Interno Django)
-- Migra miembros (relacion interesado-agrupacion) desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_miembros (query cca_miembros.sql)
-- Destino: col_miembros (modelo interno Django)
--
-- Diferencias clave con modelo CCA:
--   - PK es 'id' auto-generado con secuencia
--   - FK en Django se llama 'interesado_gc_interesado' (no solo 'interesado')
--   - participacion es numeric(11,10) en ambos modelos (0.0 a 1.0)
--
-- Dependencias:
--   - Requiere que gc_interesado ya este migrado (FASE 16)
--   - Requiere que gc_agrupacioninteresados ya este migrado (FASE 17)
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_interesado y gc_agrupacioninteresados

INSERT INTO {schema}.col_miembros (
    id,
    participacion,
    agrupacion,
    interesado_gc_interesado
)
SELECT
    -- id: Secuencia auto-generada
    nextval('{schema}.col_miembros_id_seq'),

    -- participacion
    m.participacion::numeric,

    -- agrupacion: FK directa por CCA id
    (SELECT ga.id FROM {schema}.gc_agrupacioninteresados ga
     WHERE ga.id = m.cca_agrupacion_id
     LIMIT 1),

    -- interesado_gc_interesado: FK directa por CCA id
    (SELECT gi.id FROM {schema}.gc_interesado gi
     WHERE gi.id = m.cca_interesado_id
     LIMIT 1)

FROM tmp_cca_miembros m
WHERE EXISTS (
    SELECT 1 FROM {schema}.gc_interesado gi
    WHERE gi.id = m.cca_interesado_id
)
AND EXISTS (
    SELECT 1 FROM {schema}.gc_agrupacioninteresados ga
    WHERE ga.id = m.cca_agrupacion_id
);
