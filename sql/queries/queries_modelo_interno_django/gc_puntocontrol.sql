SELECT
    pc.id,
    CASE
        WHEN pc.espacio_de_nombres IS NULL THEN 'GC_PUNTOCONTROL'
        ELSE pc.espacio_de_nombres
    END as espacio_de_nombres,
    pc.local_id,
    CASE
        WHEN pc.comienzo_vida_util_version IS NULL THEN now()
        ELSE pc.comienzo_vida_util_version
    END as comienzo_vida_util_version,
    pc.fin_vida_util_version,
    pc.id_punto,
    pc.geometria,
    CASE
        WHEN pc.exactitud_horizontal IS NULL THEN 0
        ELSE pc.exactitud_horizontal
    END as exactitud_horizontal,
    CASE
        WHEN pc.exactitud_vertical IS NULL THEN 0
        ELSE pc.exactitud_vertical
    END as exactitud_vertical,
    mpt.text_code as metodo_produccion,
    pt.text_code as punto_tipo,
    pct.text_code as tipo_punto_control
FROM {schema}.gc_puntocontrol pc
LEFT JOIN {schema}.col_metodoproducciontipo mpt ON pc.metodo_produccion = mpt.id
LEFT JOIN {schema}.gc_puntotipo pt ON pc.punto_tipo = pt.id
LEFT JOIN {schema}.gc_puntocontroltipo pct ON pc.tipo_punto_control = pct.id;
