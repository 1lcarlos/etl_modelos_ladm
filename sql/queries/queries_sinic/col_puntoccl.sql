SELECT
    p.id,
    p.ccl,
    p.punto_gc_puntocontrol,
    p.punto_gc_puntolevantamiento,
    p.punto_gc_puntolindero
FROM {schema}.col_puntoccl p
LEFT JOIN {schema}.gc_lindero l ON p.ccl = l.id
LEFT JOIN {schema}.gc_puntocontrol pc ON p.punto_gc_puntocontrol = pc.id
LEFT JOIN {schema}.gc_puntolevantamiento pl ON p.punto_gc_puntolevantamiento = pl.id
LEFT JOIN {schema}.gc_puntolindero pli ON p.punto_gc_puntolindero = pli.id;
