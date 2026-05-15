-- Query para extraer datos de gc_predio_informalidad para migrar a cr_predio_informalidad
-- Origen: {schema}.gc_predio_informalidad
-- Destino: sinic2.cr_predio_informalidad
-- Fecha: 2026-02-02

SELECT DISTINCT ON (pi.id)
    pi.id,
    pi.predio_formal as predio_formal_id,
    pi.predio_informal as predio_informal_id,
    COALESCE(
        ROUND(
            ST_Area(
                ST_Intersection(tf.geometria, ti.geometria)
            )::numeric, 2
        ), 0
    ) as area_interseccion
FROM {schema}.gc_predio_informalidad pi
LEFT JOIN {schema}.col_uebaunit uf ON uf.unidad = pi.predio_formal
LEFT JOIN {schema}.gc_terreno tf ON tf.id = uf.ue_gc_terreno
LEFT JOIN {schema}.col_uebaunit ui ON ui.unidad = pi.predio_informal
LEFT JOIN {schema}.gc_terreno ti ON ti.id = ui.ue_gc_terreno;
