-- Query para extraer datos de gc_prediocopropiedad para migrar a cr_predio_copropiedad
-- Origen: {schema}.gc_prediocopropiedad
-- Destino: sinic2.cr_predio_copropiedad
-- Fecha: 2026-02-02

SELECT DISTINCT ON (pc.id)
    pc.id,
    pc.coeficiente,
    pc.matriz as matriz_id,
    pc.unidad_predial as unidad_predial_id,
    COALESCE(
        ROUND(
            (pc.coeficiente * ST_Area(t.geometria))::numeric, 2
        ), 0
    ) as area_catastral_terreno_coeficiente
FROM {schema}.gc_prediocopropiedad pc
LEFT JOIN {schema}.col_uebaunit uf ON uf.unidad = pc.matriz
LEFT JOIN {schema}.gc_terreno t ON t.id = uf.ue_gc_terreno
ORDER BY pc.id;

