SELECT
    l.id,
    CASE
        WHEN l.espacio_de_nombres IS NULL THEN 'GC_LINDERO'
        ELSE l.espacio_de_nombres
    END as espacio_de_nombres,
    l.local_id,
    CASE
        WHEN l.comienzo_vida_util_version IS NULL THEN now()
        ELSE l.comienzo_vida_util_version
    END as comienzo_vida_util_version,
    l.fin_vida_util_version,
    l.localizacion_textual,
    l.geometria,
    CASE
        WHEN l.longitud IS NULL THEN 0
        ELSE l.longitud
    END as longitud
FROM {schema}.gc_lindero l;
