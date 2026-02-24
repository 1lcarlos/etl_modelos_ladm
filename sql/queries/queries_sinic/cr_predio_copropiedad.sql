-- Query para extraer datos de gc_prediocopropiedad para migrar a cr_predio_copropiedad
-- Origen: {schema}.gc_prediocopropiedad
-- Destino: sinic2.cr_predio_copropiedad
-- Fecha: 2026-02-02

SELECT DISTINCT ON (pc.id)
    pc.id,
    pc.coeficiente,
    pc.matriz as matriz_id,
    pc.unidad_predial as unidad_predial_id
FROM {schema}.gc_prediocopropiedad pc
ORDER BY pc.id;
