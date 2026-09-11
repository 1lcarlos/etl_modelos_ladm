-- Vincula fuentes espaciales con puntos de control o lindero
-- Nota: punto_gc_puntolevantamiento no existe en V3, se descarta
SELECT
    fe.local_id   AS fuente_espacial_local_id,
    pc.local_id   AS puntocontrol_local_id,
    pl.local_id   AS puntolindero_local_id
FROM {schema}.col_puntofuente pf
JOIN {schema}.gc_fuenteespacial fe ON fe.id = pf.fuente_espacial
LEFT JOIN {schema}.gc_puntocontrol  pc ON pc.id = pf.punto_gc_puntocontrol
LEFT JOIN {schema}.gc_puntolindero  pl ON pl.id = pf.punto_gc_puntolindero
WHERE pf.punto_gc_puntocontrol IS NOT NULL
   OR pf.punto_gc_puntolindero IS NOT NULL;
