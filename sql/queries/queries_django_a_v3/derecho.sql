SELECT
    COALESCE(d.espacio_de_nombres, 'GC_DERECHO') AS espacio_de_nombres,
    COALESCE(d.local_id, d.id::varchar) AS local_id,
    case
        when d.comienzo_vida_util_version is null
        then now()
        else d.comienzo_vida_util_version
    end as comienzo_vida_util_version,
    d.fin_vida_util_version,
    d.descripcion,
    d.id as id_derecho,
    d.fraccion_derecho,
    d.fecha_inicio_tenencia,
    d.baunit as id_predio,
    d.interesado_gc_agrupacioninteresados as id_agrupacion,
    d.interesado_gc_interesado as id_interesado,
    dt.text_code as tipo_derecho
FROM
    {schema}.gc_derecho d
JOIN {schema}.gc_derechotipo dt ON dt.id = d.tipo
