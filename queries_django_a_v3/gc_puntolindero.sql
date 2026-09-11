SELECT
    pl.id,
    CASE
        WHEN pl.espacio_de_nombres IS NULL THEN 'GC_PUNTOLINDERO'
        ELSE pl.espacio_de_nombres
    END as espacio_de_nombres,
    pl.local_id,
    CASE
        WHEN pl.comienzo_vida_util_version IS NULL THEN now()
        ELSE pl.comienzo_vida_util_version
    END as comienzo_vida_util_version,
    pl.fin_vida_util_version,
    pl.id_punto,
    pl.geometria,
    CASE
        WHEN pl.exactitud_horizontal IS NULL THEN 0
        ELSE pl.exactitud_horizontal
    END as exactitud_horizontal,
    CASE
        WHEN pl.exactitud_vertical IS NULL THEN 0
        ELSE pl.exactitud_vertical
    END as exactitud_vertical,
    at.text_code as acuerdo,
    fit.text_code as fotoidentificacion,
    mpt.text_code as metodo_produccion,
    pt.text_code as punto_tipo
FROM {schema}.gc_puntolindero pl
LEFT JOIN {schema}.gc_acuerdotipo at ON pl.acuerdo = at.id
LEFT JOIN {schema}.gc_fotoidentificaciontipo fit ON pl.fotoidentificacion = fit.id
LEFT JOIN {schema}.col_metodoproducciontipo mpt ON pl.metodo_produccion = mpt.id
LEFT JOIN {schema}.gc_puntotipo pt ON pl.punto_tipo = pt.id;
